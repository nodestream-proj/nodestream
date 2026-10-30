import ast
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from nodestream.pipeline import object_storage
from nodestream.pipeline.extractors import credential_utils, files
from nodestream.pipeline.extractors.stores.aws import dynamodb_extractor
from nodestream.pipeline.extractors.streams import kafka


def run_import_guards(module, unavailable: str) -> dict:
    """Run a module's top-level try/except ImportError blocks without `unavailable`.

    The blocks run in a fresh namespace. Re-importing the whole module would
    register its classes in the plugin registries a second time.
    """
    tree = ast.parse(Path(module.__file__).read_text())
    guards = [
        node
        for node in tree.body
        if isinstance(node, ast.Try)
        and any(getattr(h.type, "id", None) == "ImportError" for h in node.handlers)
    ]
    assert guards, f"{module.__name__} has no try/except ImportError block"
    code = compile(ast.Module(body=guards, type_ignores=[]), module.__file__, "exec")
    namespace = {}
    # A None entry in sys.modules makes importing that name raise ImportError.
    with patch.dict(sys.modules, {unavailable: None}):
        exec(code, namespace)
    return namespace


@pytest.mark.parametrize(
    "module, unavailable, flag",
    [
        (kafka, "confluent_kafka", "_has_kafka"),
        (credential_utils, "boto3", "_has_boto3"),
        (files, "pandas", "_has_pandas"),
    ],
    ids=["kafka", "aws", "pandas"],
)
def test_missing_optional_import_clears_flag(module, unavailable, flag):
    assert run_import_guards(module, unavailable)[flag] is False


@pytest.mark.parametrize(
    "module, unavailable, name",
    [
        (dynamodb_extractor, "boto3.dynamodb.types", "TypeDeserializer"),
        (object_storage, "botocore.exceptions", "ClientError"),
    ],
    ids=["dynamodb", "object_storage"],
)
def test_missing_optional_import_is_tolerated(module, unavailable, name):
    assert name not in run_import_guards(module, unavailable)
