from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar, cast

import boto3
from botocore.exceptions import ClientError

from nodestream.pipeline.argument_resolvers import ArgumentResolver

# Type variables for decorators
T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

# Configure structured logging
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SecretResolverConfig:
    """Configuration for the SecretResolver.

    Attributes:
        cache_ttl: Time-to-live for cache entries in seconds
        max_retries: Maximum number of retries for AWS API calls
        retry_delay: Delay between retries in seconds
        region_name: AWS region name
        log_level: Logging level
    """

    cache_ttl: int = 300  # 5 minutes
    max_retries: int = 3
    retry_delay: float = 1.0
    # todo get region from environment variable or config
    region_name: str = "us-west-2"
    log_level: str = "INFO"


class SecretResolverError(Exception):
    """Base exception for SecretResolver errors."""

    pass


class SecretNotFoundError(SecretResolverError):
    """Raised when a secret is not found in AWS Secrets Manager."""

    pass


class SecretDecodeError(SecretResolverError):
    """Raised when there is an error decoding a secret."""

    pass


class SecretCacheError(SecretResolverError):
    """Raised when there is an error with the secret cache."""

    pass


def retry_on_error(max_retries: int = 3, delay: float = 1.0) -> Callable[[F], F]:
    """Decorator to retry a function on failure.

    Args:
        max_retries: Maximum number of retries
        delay: Delay between retries in seconds

    Returns:
        Decorated function that will retry on failure

    Example:
        @retry_on_error(max_retries=3, delay=1.0)
        def my_function():
            # Function that may fail
            pass
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        msg = (
                            f"Attempt {attempt + 1} failed for {func.__name__}, "
                            f"retrying in {delay} seconds... Error: {str(e)}"
                        )
                        logger.warning(msg)
                        time.sleep(delay)
            raise last_exception or Exception("Unknown error occurred")

        return cast(F, wrapper)

    return decorator


class SecretCache:
    """Thread-safe cache for secrets with TTL.

    This class implements a thread-safe cache with time-to-live (TTL) for
    storing and retrieving secrets. It uses a lock to ensure thread safety
    and automatically removes expired entries.

    Attributes:
        _ttl: Time-to-live for cache entries in seconds
        _lock: Thread lock for thread safety
        _cache: Dictionary storing cache entries with expiry timestamps
    """

    def __init__(self, ttl: int = 300) -> None:
        """Initialize the secret cache.

        Args:
            ttl: Time-to-live for cache entries in seconds
        """
        self._ttl = ttl
        self._lock = threading.Lock()
        self._cache: Dict[str, tuple[Any, float]] = {}

    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache if it exists and is not expired.

        Args:
            key: Cache key

        Returns:
            Cached value if it exists and is not expired, None otherwise
        """
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if time.time() < expiry:
                    logger.debug(f"Cache HIT: {key}")
                    return value
                logger.debug(f"Cache EXPIRED: {key}")
                del self._cache[key]
            logger.debug(f"Cache MISS: {key}")
            return None

    def set(self, key: str, value: Any) -> None:
        """Set a value in the cache with TTL.

        Args:
            key: Cache key
            value: Value to cache
        """
        with self._lock:
            self._cache[key] = (value, time.time() + self._ttl)
            msg = f"Cache SET: {key} (expires in {self._ttl} seconds)"
            logger.debug(msg)


# Initialize caches
secret_cache = SecretCache()
json_cache = SecretCache()


class AWSSecretResolver(ArgumentResolver, alias="aws-secret"):  # type: ignore[call-arg]
    """AWS Secrets Manager argument resolver for Nodestream with caching and retries.

    This resolver fetches secrets from AWS Secrets Manager and caches them for
    performance. It supports both string secrets and JSON secrets with specific
    key extraction. It implements a singleton pattern to ensure a single instance
    is used throughout the application.

    Example usage in nodestream.yaml:
        password:
          resolver: aws-secret
          variable: NEO4J_PASSWORD.password  # For JSON secrets
          # OR
          variable: NEO4J_PASSWORD  # For string secrets

    Attributes:
        _instance: Singleton instance of the resolver
        config: Configuration for the resolver
        _session: AWS session
        _client: AWS Secrets Manager client
    """

    _instance: Optional[AWSSecretResolver] = None

    def __new__(
        cls, config: Optional[SecretResolverConfig] = None
    ) -> AWSSecretResolver:
        """Ensure singleton instance.

        Args:
            config: Optional configuration for the resolver

        Returns:
            Singleton instance of SecretResolver
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.config = config or SecretResolverConfig()
            cls._instance._session = boto3.session.Session()
            cls._instance._client = cls._instance._session.client(
                service_name="secretsmanager",
                region_name=cls._instance.config.region_name,
            )
        return cls._instance

    def __init__(self, config: Optional[SecretResolverConfig] = None) -> None:
        """Initialize the SecretResolver.

        Args:
            config: Optional configuration for the resolver
        """
        # Skip initialization if instance already exists
        if hasattr(self, "config"):
            return

        self.config = config or SecretResolverConfig()
        self._session = boto3.session.Session()
        self._client = self._session.client(
            service_name="secretsmanager", region_name=self.config.region_name
        )

    @staticmethod
    def _get_secret_name_from_env(env_var: str) -> Optional[str]:
        """Get secret name from environment variable.

        Args:
            env_var: Environment variable name

        Returns:
            Secret name if environment variable exists and is not empty, None otherwise
        """
        secret_name = os.environ.get(env_var)
        if not secret_name:
            logger.error(f"Environment variable '{env_var}' is not set or is empty")
            return None
        return secret_name

    @retry_on_error()
    def _get_secret_from_aws(self, secret_name: str) -> str:
        """Fetch a secret from AWS Secrets Manager.

        Args:
            secret_name: Name of the secret to fetch

        Returns:
            Secret value as string

        Raises:
            SecretNotFoundError: If the secret is not found
            SecretDecodeError: If the secret cannot be decoded
        """
        try:
            response = self._client.get_secret_value(SecretId=secret_name)
            if "SecretString" in response:
                return response["SecretString"]  # type: ignore[no-any-return]
            raise SecretDecodeError(
                f"Secret '{secret_name}' is binary, which is not supported"
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise SecretNotFoundError(f"Secret '{secret_name}' not found") from e
            raise SecretResolverError(
                f"Error fetching secret '{secret_name}': {e}"
            ) from e

    def _resolve_string_secret(self, secret_name: str) -> Optional[str]:
        """Resolve a string secret with caching.

        Args:
            secret_name: Name of the secret to resolve

        Returns:
            Secret value if successful, None otherwise
        """
        logger.info(f"Resolving string secret '{secret_name}'")

        # Try cache first
        cached_secret = secret_cache.get(secret_name)
        if cached_secret is not None:
            return cached_secret  # type: ignore[no-any-return]

        try:
            # Fetch and cache
            secret_value = self._get_secret_from_aws(secret_name)
            secret_cache.set(secret_name, secret_value)
            logger.info(f"Cached string secret '{secret_name}'")
            return secret_value
        except SecretResolverError as e:
            logger.error(f"Error resolving string secret '{secret_name}': {e}")
            return None

    def _resolve_json_secret(self, secret_name: str, json_key: str) -> Optional[Any]:
        """Resolve a JSON secret with caching.

        Args:
            secret_name: Name of the secret to resolve
            json_key: Key to extract from the JSON secret

        Returns:
            JSON value if successful, None otherwise
        """
        logger.info(f"Resolving JSON secret '{secret_name}' with key '{json_key}'")

        cache_key = f"{secret_name}:{json_key}"

        # Try JSON cache first
        cached_json = json_cache.get(cache_key)
        if cached_json is not None:
            return cached_json

        try:
            # Get the secret string
            secret_json_string = self._get_secret_from_aws(secret_name)

            # Parse JSON
            try:
                secret_data = json.loads(secret_json_string)
            except json.JSONDecodeError as e:
                raise SecretDecodeError(
                    f"Secret '{secret_name}' is not valid JSON: {e}"
                ) from e

            # Extract and cache the JSON value
            if json_key not in secret_data:
                raise SecretNotFoundError(
                    f"Key '{json_key}' not found in secret '{secret_name}'"
                )

            json_cache.set(cache_key, secret_data[json_key])
            logger.info(f"Cached JSON key '{json_key}' from secret '{secret_name}'")
            return secret_data[json_key]

        except SecretResolverError as e:
            logger.error(f"Error resolving JSON secret '{secret_name}': {e}")
            return None

    @classmethod
    def resolve_argument(cls, variable_name: str) -> Optional[Any]:
        """Resolve an argument by fetching it from AWS Secrets Manager with caching.

        This method is called by the nodestream plugin system to resolve arguments
        that use the !aws-secret tag in the configuration.

        Supports two formats:
        1. 'ENV_VAR_NAME.json_key' - For JSON secrets, returns the specific JSON key value
        2. 'ENV_VAR_NAME' - For string secrets, returns the entire secret value

        Args:
            variable_name: The variable name in either format:
                         - 'ENV_VAR_NAME.json_key' for JSON secrets
                         - 'ENV_VAR_NAME' for string secrets

        Returns:
            The resolved value from the secret, or None if resolution failed

        Example:
            In nodestream.yaml:
                password: !aws-secret NEO4J_PASSWORD.password
                # OR
                password: !aws-secret NEO4J_PASSWORD
        """
        instance = cls()  # Get singleton instance
        try:
            # Split the variable name into parts
            parts = variable_name.split(".", 1)
            env_var_part = parts[0]
            json_key_part = parts[1] if len(parts) > 1 else None

            # Get secret name from environment variable
            secret_name = instance._get_secret_name_from_env(env_var_part)
            if not secret_name:
                return None

            # Resolve based on type
            if json_key_part is None:
                return instance._resolve_string_secret(secret_name)
            return instance._resolve_json_secret(secret_name, json_key_part)

        except Exception as e:
            logger.error(
                f"Unexpected error resolving '{variable_name}': {e}", exc_info=True
            )
            return None

    def get_value(self) -> str:
        """Return the secret value (or a default if not found)."""
        return self._secret_value or self.default  # type: ignore[no-any-return]

    def get_secret_value(self, secret_id: str) -> str | None:
        """Return the secret value (or None if not found)."""
        return self._secret_value or None
