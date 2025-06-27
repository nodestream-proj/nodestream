from datetime import datetime
from time import time
from typing import Optional
from uuid import uuid4

import boto3
import pytz
from botocore.credentials import RefreshableCredentials
from botocore.session import Session


class AwsClientFactory:
    def __init__(
        self,
        assume_role_arn: Optional[str] = None,
        assume_role_external_id: Optional[str] = None,
        session_ttl: int = 3000,
        **boto_session_args,
    ) -> None:
        self.assume_role_arn = assume_role_arn
        self.assume_role_external_id = assume_role_external_id
        self.session_args = self._init_session_args(**boto_session_args)
        self.session_ttl = session_ttl

    @staticmethod
    def _init_session_args(**boto_session_args):
        session_args = boto_session_args.copy() if boto_session_args else {}
        if "profile_name" in session_args and not session_args["profile_name"]:
            del session_args["profile_name"]
        return session_args

    def get_credentials_from_assume_role(self):
        sts_client = boto3.client("sts", **self.session_args)
        role_session_name = str(uuid4())
        assume_role_kwargs = {
            "RoleArn": self.assume_role_arn,
            "RoleSessionName": role_session_name,
        }
        if self.assume_role_external_id:
            assume_role_kwargs["ExternalId"] = self.assume_role_external_id
        assumed_role_object = sts_client.assume_role(**assume_role_kwargs)
        return {
            "access_key": assumed_role_object["Credentials"]["AccessKeyId"],
            "secret_key": assumed_role_object["Credentials"]["SecretAccessKey"],
            "token": assumed_role_object["Credentials"]["SessionToken"],
            "expiry_time": assumed_role_object["Credentials"]["Expiration"].isoformat(),
        }

    def get_credentials_from_provider_chain(self):
        session = Session()
        session_credentials = session.get_credentials().get_frozen_credentials()
        return {
            "access_key": session_credentials.access_key,
            "secret_key": session_credentials.secret_key,
            "token": session_credentials.token,
            "expiry_time": datetime.fromtimestamp(
                time() + self.session_ttl, tz=pytz.utc
            ).isoformat(),
        }

    def get_boto_session_with_refreshable_credentials(self):
        creds = (
            self.get_credentials_from_assume_role
            if self.assume_role_arn
            else self.get_credentials_from_provider_chain
        )

        refreshable_credentials = RefreshableCredentials.create_from_metadata(
            metadata=creds(),
            refresh_using=creds,
            method="sts-assume-role",
        )
        session = Session()
        session._credentials = refreshable_credentials
        return session

    def make_client(self, client_name: str):
        botocore_session = self.get_boto_session_with_refreshable_credentials()
        return boto3.Session(
            botocore_session=botocore_session, **self.session_args
        ).client(client_name)
