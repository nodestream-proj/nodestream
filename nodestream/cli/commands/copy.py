from logging import getLogger
from typing import Dict

from cleo.helpers import option

from ...metrics import Metrics
from ...project import Project, Target
from ...schema import Schema
from ..operations import (
    InitializeLogger,
    InitializeMetricsHandler,
    InitializeProject,
    RunCopy,
)
from ..operations.run_pipeline import create_progress_reporter
from .nodestream_command import NodestreamCommand
from .shared_options import JSON_OPTION, PROJECT_FILE_OPTION, PROMETHEUS_OPTIONS

logger = getLogger(__name__)


class UnknownTargetError(Exception):
    pass


class Copy(NodestreamCommand):
    name = "copy"
    description = "Copy Data from one target to another"
    options = [
        PROJECT_FILE_OPTION,
        JSON_OPTION,
        option("from", "f", "The target to copy from", flag=False),
        option("to", "t", "The target to copy to", flag=False),
        option(
            "node",
            description="Filter to these node types (omit for all)",
            flag=False,
            multiple=True,
        ),
        option(
            "relationship",
            description="Filter to these relationship types (omit for all)",
            flag=False,
            multiple=True,
        ),
        option(
            "concurrency-limit",
            "c",
            "Number of concurrent copy workers (1 = sequential)",
            default=1,
            flag=False,
        ),
        option(
            "batch-size",
            description="Number of records per writer batch",
            default=1000,
            flag=False,
        ),
        option(
            "step-outbox-size",
            description="Buffer size between pipeline steps",
            default=10000,
            flag=False,
        ),
        option(
            "flush-concurrency",
            description="Number of concurrent flush lanes in the writer",
            default=1,
            flag=False,
        ),
        option(
            "connector-option",
            description="key=value connector override (repeatable)",
            flag=False,
            multiple=True,
        ),
        option(
            "reporting-frequency",
            description="How often to report progress (every N records)",
            default=1000,
            flag=False,
        ),
        option(
            "metrics-interval-in-seconds",
            description="Time interval to report metrics in seconds",
            default=None,
            flag=False,
        ),
        option(
            "retriever-option",
            description="key=value type retriever parameter (repeatable, e.g. limit=1000)",
            flag=False,
            multiple=True,
        ),
        option(
            "shard-size",
            description=(
                "Split each type into fixed-size shards of this many records, "
                "interleaved across types for sustained concurrency. "
                "Requires the type retriever to support sharding. "
                "Disabled when not set."
            ),
            default=None,
            flag=False,
        ),
        option(
            "preload-nodes",
            description=(
                "Copy nodes first, then relationships. "
                "Useful when the destination needs nodes seeded before adjacencies are written."
            ),
            flag=True,
        ),
        *PROMETHEUS_OPTIONS,
    ]

    async def handle_async(self):
        with Metrics.capture():
            await self.run_operation(InitializeLogger())
            await self.run_operation(InitializeMetricsHandler())
            project = await self.run_operation(InitializeProject())

            try:
                from_target = self.get_taget_from_user(project, "from")
                to_target = self.get_taget_from_user(project, "to")
                preloadNodes = bool(self.option("preload-nodes"))

                full_schema = project.make_schema_for_copy(
                    include_additional_types=False
                )
                schema = self.apply_schema_filter(full_schema)

                batch_size = int(self.option("batch-size"))
                step_outbox_size = int(self.option("step-outbox-size"))
                flush_concurrency = int(self.option("flush-concurrency"))
                concurrency_limit = int(self.option("concurrency-limit"))
                shardSizeRaw = self.option("shard-size")
                connector_overrides = self.parse_key_value_options("connector-option")
                retriever_overrides = self.parse_key_value_options("retriever-option")

                retriever_overrides.setdefault("preload_nodes", preloadNodes)
                if shardSizeRaw is not None:
                    retriever_overrides.setdefault("shard_size", int(shardSizeRaw))
            except UnknownTargetError:
                return 1

            logger.info(
                "Starting copy",
                extra={
                    "from": from_target.name,
                    "to": to_target.name,
                    "preload_nodes": preloadNodes,
                    "node_types": [nodeType.name for nodeType in schema.nodes],
                    "relationship_types": [
                        relationshipType.name
                        for relationshipType in schema.relationships
                    ],
                    "retriever_overrides": retriever_overrides,
                    "connector_overrides": connector_overrides,
                },
            )
            reporter = create_progress_reporter(self, "copy")
            await self.run_operation(
                RunCopy(
                    from_target=from_target,
                    to_target=to_target,
                    schema=schema,
                    progress_reporter=reporter,
                    batch_size=batch_size,
                    step_outbox_size=step_outbox_size,
                    flush_concurrency=flush_concurrency,
                    concurrency_limit=concurrency_limit,
                    connector_overrides=connector_overrides,
                    retriever_overrides=retriever_overrides,
                )
            )
        return 0

    def apply_schema_filter(self, schema: Schema) -> Schema:
        """Return a filtered schema based on --node and --relationship flags.

        No flags: full schema (all nodes + all adjacencies).
        --node A B: nodes A and B plus all adjacencies touching either,
                    restricted by --relationship if also provided.
        --relationship X Y: all adjacencies of type X or Y (and their endpoint nodes),
                            restricted to nodes in --node if also provided.
        """
        node_filter = []
        rel_filter = []

        all_node_names = {nodeType.name for nodeType in schema.nodes}
        for nodeTypeName in self.option("node") or []:
            if nodeTypeName in all_node_names:
                node_filter.append(nodeTypeName)
            else:
                logger.warning("Unknown node type %r — skipping", nodeTypeName)

        all_rel_names = {
            relationshipType.name for relationshipType in schema.relationships
        }
        for relationshipTypeName in self.option("relationship") or []:
            if relationshipTypeName in all_rel_names:
                rel_filter.append(relationshipTypeName)
            else:
                logger.warning(
                    "Unknown relationship type %r — skipping", relationshipTypeName
                )

        return schema.filtered(node_filter=node_filter, relationship_filter=rel_filter)

    def parse_key_value_options(self, option_name: str) -> Dict[str, object]:
        raw = self.option(option_name) or []
        overrides: Dict[str, object] = {}
        for item in raw:
            key, _, value = item.partition("=")
            if value.lower() == "true":
                value = True
            elif value.lower() == "false":
                value = False
            else:
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        pass
            overrides[key] = value
        return overrides

    def get_taget_from_user(self, project: Project, action: str) -> Target:
        if (choice := self.option(action)) is None:
            prompt = f"Which target would you like to copy {action}?"
            choices = [t for t in project.targets_by_name.keys()]
            choice = self.choice(prompt, choices)

        try:
            return project.get_target_by_name(choice)
        except ValueError:
            self.line_error(f"Unknown target: {choice}")
            raise UnknownTargetError
