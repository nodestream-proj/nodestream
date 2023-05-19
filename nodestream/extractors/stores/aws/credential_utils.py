from typing import Optional
from uuid import uuid4

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.session import Session


class AwsClientFactory:
    def __init__(
        self,
        assume_role_arn: Optional[str] = None,
        assume_role_external_id: Optional[str] = None,
        **boto_session_args
    ) -> None:
        self.assume_role_arn = assume_role_arn
        self.assume_role_external_id = assume_role_external_id
        self.session_args = self._init_session_args(**boto_session_args)

    @staticmethod
    def _init_session_args(**boto_session_args):
        session_args = boto_session_args.copy() if boto_session_args else {}
        if "profile_name" in session_args and not session_args["profile_name"]:
            del session_args["profile_name"]
        return session_args

    def assume_role_and_get_credentials(self):
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

    def get_boto_session_with_refreshable_credentials(self):
        refreshable_credentials = RefreshableCredentials.create_from_metadata(
            metadata=self.assume_role_and_get_credentials(),
            refresh_using=self.assume_role_and_get_credentials,
            method="sts-assume-role",
        )
        session = Session(**self.session_args)
        session._credentials = refreshable_credentials
        return session

    def assume_role_if_supplied_and_get_session(self):
        if self.assume_role_arn:
            return self.get_boto_session_with_refreshable_credentials()
        return Session(**self.session_args)

    def make_client(self, client_name: str):
        session = self.assume_role_if_supplied_and_get_session()
        return boto3.Session(botocore_session=session).client(client_name)
