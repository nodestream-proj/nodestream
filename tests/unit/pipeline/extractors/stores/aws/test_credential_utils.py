import datetime

import pytest
from hamcrest import assert_that, equal_to, has_key, instance_of, not_


@pytest.fixture
def client_with_role():
    from nodestream.pipeline.extractors.credential_utils import AwsClientFactory

    return AwsClientFactory(assume_role_arn="arn:aws:iam::123456789012:role/test")


@pytest.fixture
def client_without_role():
    from nodestream.pipeline.extractors.credential_utils import AwsClientFactory

    return AwsClientFactory()


def test_init_extra_args_profile():
    from nodestream.pipeline.extractors.credential_utils import AwsClientFactory

    extra_args = AwsClientFactory._init_session_args(profile_name="test")
    assert_that(extra_args, has_key("profile_name"))


def test_init_extra_args_empty_profile():
    from nodestream.pipeline.extractors.credential_utils import AwsClientFactory

    extra_args = AwsClientFactory._init_session_args(profile_name="")
    assert_that(extra_args, not_(has_key("profile_name")))


def test_init_extra_args_no_profile():
    from nodestream.pipeline.extractors.credential_utils import AwsClientFactory

    extra_args = AwsClientFactory._init_session_args()
    assert_that(extra_args, not_(has_key("profile_name")))


def test_assume_role_and_get_credentials(mocker, client_with_role):
    mock_sts_client = mocker.patch(
        "nodestream.pipeline.extractors.credential_utils.boto3.client"
    )
    mock_sts_client.return_value.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "test_access_key",
            "SecretAccessKey": "test_secret_key",
            "SessionToken": "test_token",
            "Expiration": datetime.datetime(2020, 1, 1),
        }
    }
    credentials = client_with_role.assume_role_and_get_credentials()
    assert_that(credentials["access_key"], equal_to("test_access_key"))


def test_get_boto_session_with_refreshable_credentials(mocker, client_with_role):
    # mock assume_role_and_get_credentials and assert that a session was created with refreshable credentials
    client_with_role.assume_role_and_get_credentials = mocker.MagicMock(
        return_value={
            "access_key": "test_access_key",
            "secret_key": "test_secret_key",
            "token": "test_token",
            "expiry_time": datetime.datetime.now().isoformat(),
        }
    )
    session = client_with_role.get_boto_session_with_refreshable_credentials()
    assert_that(session._credentials.method, equal_to("sts-assume-role"))


def test_assume_role_if_supplied_and_get_session(mocker, client_with_role):
    # create a AwsClientFactory with a role arn and mock get_boto_session_with_refreshable_credentials.
    # assert that a the result of get_boto_session_with_refreshable_credentials is what is returned.
    client_with_role.get_boto_session_with_refreshable_credentials = mocker.MagicMock(
        return_value="test_session"
    )
    session = client_with_role.assume_role_if_supplied_and_get_session()
    assert_that(session, equal_to("test_session"))


def test_assume_role_if_supplied_and_get_session_no_role_arn(
    mocker, client_without_role
):
    from botocore.session import Session

    # create a AwsClientFactory without a role arn and mock get_boto_session_with_refreshable_credentials.
    # assert that a the result of get_boto_session_with_refreshable_credentials is what is returned.
    client_without_role.get_boto_session_with_refreshable_credentials = (
        mocker.MagicMock()
    )
    session = client_without_role.assume_role_if_supplied_and_get_session()
    client_without_role.get_boto_session_with_refreshable_credentials.assert_not_called()
    assert_that(session, instance_of(Session))


def test_make_client(mocker, client_without_role):
    # mock the clal to assume_role_if_supplied_and_get_session.
    # assert that a client was created with the session returned from assume_role_if_supplied_and_get_session.
    from botocore.session import Session

    session = Session()
    client_without_role.assume_role_if_supplied_and_get_session = mocker.MagicMock(
        return_value=session
    )
    client_without_role.make_client("s3")
