import base64
from pathlib import Path

import pytest
from botocore.exceptions import ClientError
from hamcrest import assert_that, equal_to, is_, none, not_none

from nodestream.pipeline.object_storage import (
    DirectoryObjectStore,
    HmacSigner,
    InvalidSignatureError,
    MalformedSignedObjectError,
    NullObjectStore,
    ObjectStore,
    S3ObjectStore,
    SignedObject,
    StaticNamespace,
)

SOME_KEY = "some_key"
SOME_DATA = b"some_data"
BUCKET_NAME = "bucket_name"


@pytest.fixture
def data():
    return b"test_data"


@pytest.fixture
def directory_object_store(tmp_path):
    return DirectoryObjectStore(tmp_path)


@pytest.fixture
def namespaced_object_store(directory_object_store):
    return directory_object_store.namespaced("prefix")


@pytest.fixture
def hmac_signer():
    return HmacSigner.from_base64("waPmETwMNZlVLq/VY3i0yg==")


@pytest.fixture
def signed_object_store(directory_object_store, hmac_signer):
    return directory_object_store.signed(hmac_signer)


def test_signed_object():
    data = b"data"
    signature = b"signature"
    signed_object = SignedObject(signature, data)
    assert signed_object.signature == signature
    assert signed_object.data == data

    bytes_data = signed_object.into_bytes()
    assert bytes_data == signature + b"\n" + data

    parsed_object = SignedObject.from_bytes(bytes_data)
    assert_that(parsed_object, is_(not_none()))
    assert_that(parsed_object.signature, equal_to(signature))
    assert_that(parsed_object.data, equal_to(data))


def test_hmac_signer_correct(hmac_signer):
    signed_object = hmac_signer.sign(SOME_DATA)
    assert_that(signed_object.data, equal_to(SOME_DATA))
    hmac_signer.verify(signed_object)


def test_hmac_signer_incorrect(hmac_signer):
    with pytest.raises(InvalidSignatureError):
        hmac_signer.verify(SignedObject(b"invalid_signature", SOME_DATA))


def test_static_namespace():
    namespace = StaticNamespace("prefix")
    scoped_key = namespace.scope("key")
    assert_that(scoped_key, equal_to("prefix/key"))


def test_directory_object_store_found_object(directory_object_store):
    directory_object_store.put(SOME_KEY, SOME_DATA)
    retrieved_data = directory_object_store.get(SOME_KEY)
    assert_that(retrieved_data, equal_to(SOME_DATA))


def test_directory_object_store_missing_object(directory_object_store):
    retrieved_data = directory_object_store.get(SOME_KEY)
    assert_that(retrieved_data, is_(none()))


def test_directory_object_store_delete(directory_object_store):
    directory_object_store.put(SOME_KEY, SOME_DATA)
    directory_object_store.delete(SOME_KEY)
    assert_that(directory_object_store.get(SOME_KEY), is_(none()))


def test_directory_object_store_get_pickled(directory_object_store):
    data = {"data": SOME_DATA}
    directory_object_store.put_picklable(SOME_KEY, data)
    retrieved_data = directory_object_store.get_pickled(SOME_KEY)
    assert_that(retrieved_data, equal_to(data))


def test_get_pickled_missing_object(directory_object_store):
    retrieved_data = directory_object_store.get_pickled(SOME_KEY)
    assert_that(retrieved_data, is_(none()))


def test_directory_object_store_default_directory():
    store = ObjectStore.in_current_directory()
    assert_that(store.root, equal_to(Path.cwd() / ".nodestream" / "objects"))


def test_null_object_store():
    store = NullObjectStore()
    store.put(SOME_KEY, SOME_DATA)
    retrieved_data = store.get(SOME_KEY)
    assert_that(retrieved_data, is_(none()))


def test_namespaced_object_store(namespaced_object_store):
    namespaced_object_store.put(SOME_KEY, SOME_DATA)
    retrieved_data = namespaced_object_store.get(SOME_KEY)
    assert retrieved_data == SOME_DATA


def test_namespaced_object_store_delete(namespaced_object_store):
    namespaced_object_store.put(SOME_KEY, SOME_DATA)
    namespaced_object_store.delete(SOME_KEY)
    assert_that(namespaced_object_store.get(SOME_KEY), is_(none()))


def test_signed_object_store(signed_object_store):
    signed_object_store.put(SOME_KEY, SOME_DATA)
    retrieved_data = signed_object_store.get(SOME_KEY)
    assert_that(retrieved_data, equal_to(SOME_DATA))


def test_signed_object_store_detects_tampering(tmp_path, signed_object_store):
    signed_object_store.put(SOME_KEY, SOME_DATA)

    # Tamper with the data
    obj_path = Path(tmp_path) / SOME_KEY
    with open(obj_path, "rb") as f:
        signed_data = f.read()

    signature, _ = signed_data.split(b"\n")
    tampered_data = base64.b64encode(b"tampered_data")
    tampered_signed_data = signature + b"\n" + tampered_data

    with open(obj_path, "wb") as f:
        f.write(tampered_signed_data)

    with pytest.raises(InvalidSignatureError):
        signed_object_store.get(SOME_KEY)


def test_signed_object_store_detects_missing_signature(
    directory_object_store, signed_object_store
):
    directory_object_store.put(SOME_KEY, SOME_DATA)
    with pytest.raises(MalformedSignedObjectError):
        signed_object_store.get(SOME_KEY)


def test_signed_object_store_missing_object(signed_object_store):
    assert_that(signed_object_store.get(SOME_KEY), is_(none()))


def test_signed_object_store_delete(signed_object_store):
    signed_object_store.put(SOME_KEY, SOME_DATA)
    signed_object_store.delete(SOME_KEY)
    assert_that(signed_object_store.get(SOME_KEY), is_(none()))


@pytest.fixture
def s3_client(mocker):
    return mocker.MagicMock()


@pytest.fixture
def s3_object_store(s3_client):
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "nodestream.pipeline.object_storage.AwsClientFactory.make_client",
            lambda self, service: s3_client,
        )
        yield S3ObjectStore(BUCKET_NAME)


def test_s3_object_store_get_found(s3_object_store, s3_client, mocker):
    s3_client.get_object.return_value = {
        "Body": mocker.MagicMock(read=lambda: SOME_DATA)
    }
    retrieved_data = s3_object_store.get(SOME_KEY)
    assert_that(retrieved_data, equal_to(SOME_DATA))
    s3_client.get_object.assert_called_once_with(Bucket=BUCKET_NAME, Key=SOME_KEY)


def test_s3_object_store_get_not_found(s3_object_store, s3_client):
    s3_client.get_object.side_effect = ClientError(
        {"ResponseMetadata": {"HTTPStatusCode": 404}}, "get_object"
    )
    retrieved_data = s3_object_store.get(SOME_KEY)
    assert_that(retrieved_data, is_(none()))
    s3_client.get_object.assert_called_once_with(Bucket=BUCKET_NAME, Key=SOME_KEY)


def test_s3_object_store_get_other_error(s3_object_store, s3_client):
    s3_client.get_object.side_effect = ClientError(
        {"ResponseMetadata": {"HTTPStatusCode": 400}}, "get_object"
    )
    with pytest.raises(ClientError):
        s3_object_store.get(SOME_KEY)


def test_s3_object_store_put(s3_object_store, s3_client):
    s3_object_store.put(SOME_KEY, SOME_DATA)
    s3_client.put_object.assert_called_once_with(
        Bucket=BUCKET_NAME, Key=SOME_KEY, Body=SOME_DATA
    )


def test_s3_object_store_delete(s3_object_store, s3_client):
    s3_object_store.delete(SOME_KEY)
    s3_client.delete_object.assert_called_once_with(Bucket=BUCKET_NAME, Key=SOME_KEY)


def test_s3_object_store_delete_not_found(s3_object_store, s3_client):
    s3_client.delete_object.side_effect = ClientError(
        {"ResponseMetadata": {"HTTPStatusCode": 404}}, "delete_object"
    )
    s3_object_store.delete(SOME_KEY)
    s3_client.delete_object.assert_called_once_with(Bucket=BUCKET_NAME, Key=SOME_KEY)


def test_s3_object_store_delete_other_error(s3_object_store, s3_client):
    s3_client.delete_object.side_effect = ClientError(
        {"ResponseMetadata": {"HTTPStatusCode": 400}}, "delete_object"
    )
    with pytest.raises(ClientError):
        s3_object_store.delete(SOME_KEY)
