import base64

import pytest
from hamcrest import assert_that, contains, equal_to, instance_of

from nodestream.pipeline.object_storage import (
    DirectoryObjectStore,
    NullObjectStore,
    SignedObjectStore,
)
from nodestream.project.storage import StorageConfiguration, StoreConfiguration
from unittest.mock import Mock
from hamcrest import assert_that, instance_of
from nodestream.file_io import LazyLoadedArgument
from nodestream.pipeline.object_storage import SignedObjectStore, DirectoryObjectStore


@pytest.fixture
def store_config_data():
    return {
        "name": "test_store",
        "type": "test_type",
        "arg1": "value1",
        "arg2": "value2",
        "hmac_key": "test_key",
    }


@pytest.fixture
def store_config(store_config_data):
    return StoreConfiguration.from_file_data(dict(store_config_data))


def test_storage_configuration_initialize(tmp_path):
    store_config = StoreConfiguration(
        name="bob",
        storage_type="local",
        arguments={"root": tmp_path},
        hmac_key="dvHdCrVbRPp1HcmWX78Ryw==",
    )
    store = store_config.initialize()
    assert_that(store, instance_of(SignedObjectStore))
    assert_that(store.store.root, equal_to(tmp_path))

    store_config = StoreConfiguration(
        name="bob", storage_type="local", arguments={"root": tmp_path}
    )
    store = store_config.initialize()
    assert_that(store, instance_of(DirectoryObjectStore))


def test_store_configuration_to_file_data(store_config, store_config_data):
    file_data = store_config.to_file_data()
    assert_that(file_data, equal_to(store_config_data))


def test_store_configuration_from_file_data(store_config_data):
    store_config = StoreConfiguration.from_file_data(dict(store_config_data))
    assert_that(store_config.name, equal_to(store_config_data["name"]))
    assert_that(store_config.storage_type, equal_to(store_config_data["type"]))
    assert_that(store_config.arguments, equal_to({"arg1": "value1", "arg2": "value2"}))
    assert_that(store_config.hmac_key, equal_to(store_config_data["hmac_key"]))


def test_storage_configuration_initialize_by_name(mocker):
    config = mocker.MagicMock()
    storage_config = StorageConfiguration(
        storage_configuration_by_name={"test_store": config}
    )
    result = storage_config.initialize_by_name("test_store")
    assert_that(result, equal_to(config.initialize.return_value))


def test_storage_configuration_initialize_by_name_not_found(mocker):
    store_config = StorageConfiguration({})
    result = store_config.initialize_by_name("test_store")
    assert_that(result, instance_of(NullObjectStore))


def test_storage_configuration_from_file_data(store_config_data):
    data = {"stores": [store_config_data]}
    storage_config = StorageConfiguration.from_file_data(data)
    assert_that(storage_config.storage_configuration_by_name, contains("test_store"))
    assert_that(
        storage_config.storage_configuration_by_name["test_store"],
        instance_of(StoreConfiguration),
    )


def test_storage_configuration_to_file_data(store_config):
    storage_config = StorageConfiguration(
        storage_configuration_by_name={"test_store": store_config}
    )
    file_data = storage_config.to_file_data()
    expected_data = {"stores": [("test_store", store_config)]}
    assert_that(file_data, equal_to(expected_data))


def test_store_configuration_initialize_with_lazy_hmac(tmp_path):
    expected_hmac = "dvHdCrVbRPp1HcmWX78Ryw=="
    mock_lazy_hmac = Mock(spec=LazyLoadedArgument)
    mock_lazy_hmac.get_value.return_value = expected_hmac

    store_config = StoreConfiguration(
        name="test-store",
        storage_type="local",
        arguments={"root": tmp_path},
        hmac_key=mock_lazy_hmac
    )

    store = store_config.initialize()
    assert_that(
        base64.b64encode(store.signer.key).decode(),
        equal_to(expected_hmac)
    )
    mock_lazy_hmac.get_value.assert_called_once()