from pathlib import Path
from typing import List

from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation

PROJECT_MODULES_TO_CREATE = [
    {"module_name": "__init__"},
    {
        "module_name": "argument_resolvers",
        "import_statement": "from nodestream.argument_resolvers import ArgumentResolver",
        "guide_reference": "# See: https://nodestream-proj.github.io/nodestream/extending-nodestream/creating-your-own-argument-resolver",
    },
    {
        "module_name": "normalizers",
        "import_statement": "from nodestream.normalizers import Normalizer",
        "guide_reference": "# See: https://nodestream-proj.github.io/nodestream/extending-nodestream/creating-your-own-normalizer",
    },
    {
        "module_name": "value_providers",
        "import_statement": "from nodestream.value_providers import ValueProvider",
        "guide_reference": "# See: https://nodestream-proj.github.io/nodestream/extending-nodestream/creating-your-own-value-provider",
    },
]


class GeneratePythonScaffold(Operation):
    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root

    async def perform(self, _: NodestreamCommand) -> List[Path]:
        return [
            self.write_module(module_configuration)
            for module_configuration in PROJECT_MODULES_TO_CREATE
        ]

    def get_file_path(self, module_name) -> Path:
        return (
            self.project_root / self.project_root.absolute().name / f"{module_name}.py"
        )

    def write_module(self, module_config) -> Path:
        path = self.get_file_path(module_config["module_name"])
        self.ensure_path_to_file_exists(path)
        self.write_module_contents_at_path(path, module_config)
        return path

    def write_module_contents_at_path(self, path: Path, module_config):
        lines = []
        if import_statement := module_config.get("import_statement"):
            lines.append(import_statement)
            lines.append("\n")

        if guide_reference := module_config.get("guide_reference"):
            lines.append(guide_reference)

        with open(path, "w") as fp:
            fp.writelines(lines)

    def ensure_path_to_file_exists(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
