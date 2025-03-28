import base64
import hashlib
import hmac
import logging
import pickle
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, TypeVar

from botocore.exceptions import ClientError

from ..pluggable import Pluggable
from ..subclass_registry import SubclassRegistry
from .extractors.credential_utils import AwsClientFactory

logger = logging.getLogger(__name__)


OBJECT_STORE_REGISTRY = SubclassRegistry(ignore_overrides=True)
T = TypeVar("T")


class InvalidSignatureError(ValueError):
    """An error that is raised when a signature is invalid."""

    def __init__(self, expected: bytes, actual: bytes):
        expected = base64.b64encode(expected).decode("utf-8")
        actual = base64.b64encode(actual).decode("utf-8")
        super().__init__(
            f"Invalid signature. Expected: {expected}, Actual: {actual}",
        )


class MalformedSignedObjectError(ValueError):
    """An error that is raised when a signed object is malformed."""

    def __init__(self):
        super().__init__("Malformed signed object.")


@dataclass(frozen=True, slots=True)
class SignedObject:
    """A signed object is an object that has been signed by a signer."""

    signature: bytes
    data: bytes

    def into_bytes(self) -> bytes:
        """Convert the signed object to a bytestring."""
        return self.signature + b"\n" + self.data

    @staticmethod
    def from_bytes(data: bytes) -> "SignedObject":
        """Create a signed object from a bytestring."""
        try:
            signature, data = data.split(b"\n", 1)
            return SignedObject(signature, data)
        except ValueError:
            raise MalformedSignedObjectError


class Signer(ABC):
    """A signer is used to sign and verify objects in an object store."""

    @staticmethod
    def hmac(base_64_secret: str) -> "Signer":
        """Create a HMAC signature from a base64 secret."""
        return HmacSigner.from_base64(base_64_secret)

    @abstractmethod
    def sign(self, data: bytes) -> SignedObject:
        """Sign an object.

        This method should sign the given object and return a signed object
        that contains the signature and the data. The signature should be
        a bytestring.
        """
        pass

    @abstractmethod
    def verify(self, signed: SignedObject):
        """Verify a signed object.

        This method should verify the given signed object. If the signature is
        invalid, this should raise an InvalidSignatureError. If the signature
        is valid, this should do nothing.
        """
        pass


class HmacSigner(Signer):
    """A signer that uses HMAC to sign objects.

    This signer uses HMAC with SHA-256 to sign objects. The key used for
    signing is provided when the signer is created. The key should be a
    bytestring. The signer will use the key to sign objects using HMAC with
    SHA-256.
    """

    def __init__(self, key: bytes):
        self.key = key

    @staticmethod
    def from_base64(key: str):
        """Create the signer from a base64-encoded key."""
        return HmacSigner(base64.b64decode(key))

    def _get_digest(self, data: bytes) -> bytes:
        return hmac.new(self.key, data, hashlib.sha256).digest()

    def sign(self, data: bytes) -> SignedObject:
        return SignedObject(self._get_digest(data), data)

    def verify(self, signed: SignedObject):
        expected = self._get_digest(signed.data)
        if not hmac.compare_digest(signed.signature, expected):
            raise InvalidSignatureError(expected, signed.signature)


class Namespace(ABC):
    """A namespace is a way to scope keys in an object store."""

    def scope(self, key: str) -> str:
        """Scope a key.

        This method should return a new key that is scoped by the namespace.
        The new key should be unique for the given key and namespace.
        """
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class StaticNamespace(Namespace):
    """A namespace that is static and does not change."""

    prefix: str

    def scope(self, key: str) -> str:
        return f"{self.prefix}/{key}"


@OBJECT_STORE_REGISTRY.connect_baseclass
class ObjectStore(ABC, Pluggable):
    entrypoint_name = "object_stores"

    @abstractmethod
    def get(self, key: str) -> Optional[bytes]:
        """Get an object from the object store.

        The object store should return a path to the object that can be read.
        If an error is encountered, this should raise an exception.
        If the object is not found, this should return None.
        """
        pass

    @abstractmethod
    def put(self, key: str, path: bytes):
        """Put an object into the object store.

        The object store should store the object at the given path under the
        given key. If an error is encountered, this should raise an exception.
        """
        pass

    @abstractmethod
    def delete(self, key: str):
        """Delete an object from the object store.

        If the object is not found, this should do nothing.
        """
        pass

    def get_pickled(self, key: str) -> T:
        """Get an object from the object store that has been pickled."""
        if (data := self.get(key)) is None:
            return None

        return pickle.loads(data)

    def put_picklable(self, key: str, value: T):
        """Put an object from the object store that has can be pickled."""
        self.put(key, pickle.dumps(value))

    def namespaced(self, namespace: Namespace | str) -> "ObjectStore":
        """Return a new object store with a namespace applied.

        This method should return a new object store that is the same as the
        current object store, but with the given namespace applied. The new
        object store should be able to retrieve and store objects with keys
        that are scoped by the namespace.

        Note that `namespaced` can be called multiple times to apply multiple
        namespaces to the same object store. The namespaces will be applied in
        a stack-like manner, with the most recent namespace being the
        outermost.

        Args:
            namespace: The namespace to apply to the object store.
        """
        if isinstance(namespace, str):
            namespace = StaticNamespace(namespace)
        return NamespacedObjectStore(self, namespace)

    def signed(self, signer: "Signer") -> "ObjectStore":
        """Return a new object store with a signer applied.

        This method should return a new object store that is the same as the
        current object store, but with the given signer applied. The new object
        store should be able to retrieve and store signed objects.

        Args:
            signer: The signer to apply to the object store.
        """
        return SignedObjectStore(self, signer)

    @staticmethod
    def null() -> "ObjectStore":
        return NullObjectStore()

    @staticmethod
    def from_file_arguments(type: str, **arguments) -> "ObjectStore":
        object_store_type = OBJECT_STORE_REGISTRY.get(type)
        return object_store_type(**arguments)


class DirectoryObjectStore(ObjectStore, alias="local"):
    """An object store that stores objects in a directory on a file system."""

    def __init__(self, root: Optional[Path] = None) -> None:
        self.root = root or (Path.cwd() / ".nodestream" / "objects")

    def get(self, key: str) -> Optional[bytes]:
        path = self.root / key
        if not path.exists():
            return None

        with path.open("rb") as f:
            return f.read()

    def put(self, key: str, path: bytes):
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("wb") as f:
            f.write(path)

    def delete(self, key: str):
        target = self.root / key
        target.unlink(missing_ok=True)


class NullObjectStore(ObjectStore, alias="null"):
    """An object store that does not store any objects."""

    def __init__(self):
        logger.error("Using null ObjectStore. No persistence is configured.")

    def get(self, _: str) -> Optional[bytes]:
        return None

    def put(self, _path: str, _data: bytes):
        pass

    def delete(self, _path: str):
        pass


@dataclass(frozen=True, slots=True)
class NamespacedObjectStore(ObjectStore):
    """An object store that stores objects with a namespace applied.

    This object store wraps another object store and scopes keys with a
    provided namespace. This is used for ensuring that keys are sufficiently
    unique when they are stored and read from the object store.
    """

    store: ObjectStore
    namespace: Namespace

    def get(self, key: str) -> Optional[bytes]:
        return self.store.get(self.namespace.scope(key))

    def put(self, key: str, path: bytes):
        self.store.put(self.namespace.scope(key), path)

    def delete(self, key: str):
        self.store.delete(self.namespace.scope(key))


@dataclass(frozen=True, slots=True)
class SignedObjectStore(ObjectStore):
    """An object store that stores signed objects.

    This object store wraps another object store and signs and verifies objects
    that are stored and retrieved from that object store. This is used for
    ensuring that objects are not tampered with when they are stored and read
    from the object store.
    """

    store: ObjectStore
    signer: Signer

    def get(self, key: str) -> Optional[bytes]:
        if (data := self.store.get(key)) is None:
            return None

        signed = SignedObject.from_bytes(data)
        self.signer.verify(signed)
        return signed.data

    def put(self, key: str, path: bytes):
        signed = self.signer.sign(path)
        self.store.put(key, signed.into_bytes())

    def delete(self, key: str):
        self.store.delete(key)


class S3ObjectStore(ObjectStore, alias="s3"):
    __slots__ = ("client", "bucket_name")

    def __init__(self, bucket_name: str, **client_factory_args):
        client_factory = AwsClientFactory(**client_factory_args)
        self.client = client_factory.make_client("s3")
        self.bucket_name = bucket_name

    def get(self, key: str) -> Optional[bytes]:
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            return response["Body"].read()
        except ClientError as e:
            status = e.response["ResponseMetadata"]["HTTPStatusCode"]
            if status == 404:
                return None
            raise e

    def put(self, key: str, data: bytes):
        self.client.put_object(Bucket=self.bucket_name, Key=key, Body=data)

    def delete(self, key: str):
        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=key)
        except ClientError as e:
            status = e.response["ResponseMetadata"]["HTTPStatusCode"]
            if status == 404:
                return
            raise e
