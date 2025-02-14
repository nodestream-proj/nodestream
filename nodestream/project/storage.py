from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from ..pipeline.object_storage import ObjectStore, Signer


@dataclass(frozen=True, slots=True)
class StoreConfiguration:
    name: str
    storage_type: str
    arguments: Dict[str, Any]
    hmac_key: Optional[any] = None

    def initialize(self) -> ObjectStore:
        store = ObjectStore.from_file_arguments(self.storage_type, **self.arguments)
        if self.hmac_key:
            return store.signed(Signer.hmac(self.hmac_key))
        else:
            return store

    def to_file_data(self):
        return dict(
            name=self.name,
            type=self.storage_type,
            hmac_key=self.hmac_key,
            **self.arguments
        )

    @staticmethod
    def from_file_data(data):
        storage_type = data.pop("type")
        name = data.pop("name")
        hmac_key = data.pop("hmac_key", None)
        return StoreConfiguration(
            name=name, storage_type=storage_type, hmac_key=hmac_key, arguments=data
        )

    @staticmethod
    def describe_yaml_schema():
        from schema import Optional, Schema

        return Schema(
            {"name": str, "type": str, Optional("hmac_key"): str, Optional(str): object}
        )


@dataclass(frozen=True, slots=True)
class StorageConfiguration:
    storage_configuration_by_name: Dict[str, StoreConfiguration] = field(
        default_factory=dict
    )

    def initialize_by_name(self, name: str) -> ObjectStore:
        if name not in self.storage_configuration_by_name:
            return ObjectStore.null()

        return self.storage_configuration_by_name[name].initialize()

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Schema

        return Schema(
            {
                Optional("stores"): [
                    StoreConfiguration.describe_yaml_schema(),
                ]
            }
        )

    @classmethod
    def from_file_data(cls, data):
        storage_configuration_by_name = {
            store.name: store
            for store in (
                StoreConfiguration.from_file_data(store_data)
                for store_data in data.get("stores", [])
            )
        }

        return StorageConfiguration(storage_configuration_by_name)

    def to_file_data(self) -> dict:
        return {"stores": [v for v in self.storage_configuration_by_name.items()]}
