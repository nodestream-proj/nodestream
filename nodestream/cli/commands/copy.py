from logging import getLogger
from typing import Dict, List

from cleo.helpers import option

from ...metrics import Metrics
from ...project import Project, Target
from ...schema import GraphObjectSchema
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
        option("all", "a", "Copy all node and relationship types", flag=True),
        option(
            "node",
            description="Specify a node type to copy",
            flag=False,
            multiple=True,
        ),
        option(
            "relationship",
            description="Specify a relationship type to copy",
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
            "relationships-only",
            description=(
                "Skip node fetching entirely — copy only relationships. "
                "Nodes must already exist in the target; missing endpoints "
                "are silently dropped (MATCH_ONLY). Useful for e2e test "
                "partition seeding where nodes are pre-populated."
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
                relationships_only = bool(self.option("relationships-only"))

                # Always build the schema so type filters can be validated
                # against it and the retriever has full type metadata.
                schema = project.make_schema_for_copy(include_additional_types=False)
                node_types = self.resolve_type_filter(schema.nodes, "node")

                # When node types are explicitly filtered (not --all and not
                # interactive-all), narrow relationships to those whose
                # adjacency involves at least one selected node type — unless
                # the user also provided explicit --relationship filters.
                explicit_nodes = self.option("node")
                if (
                    explicit_nodes
                    and not self.option("relationship")
                    and not self.option("all")
                ):
                    node_set = set(node_types)
                    rel_types = [
                        adj.relationship_type
                        for adj in schema.adjacencies
                        if adj.from_node_type in node_set
                        or adj.to_node_type in node_set
                    ]
                    # deduplicate while preserving order
                    seen: set = set()
                    rel_types = [r for r in rel_types if not (r in seen or seen.add(r))]
                else:
                    rel_types = self.resolve_type_filter(
                        schema.relationships, "relationship"
                    )

                batch_size = int(self.option("batch-size"))
                step_outbox_size = int(self.option("step-outbox-size"))
                flush_concurrency = int(self.option("flush-concurrency"))
                concurrency_limit = int(self.option("concurrency-limit"))
                shard_size_raw = self.option("shard-size")
                connector_overrides = self.parse_key_value_options("connector-option")
                retriever_overrides = self.parse_key_value_options("retriever-option")

                # Merge CLI-level retriever knobs into retriever_overrides so that
                # make_type_retriever receives a single flat dict. Plugins pop what
                # they understand and ignore the rest — no plugin-specific kwargs
                # leak from core into the retriever contract.
                retriever_overrides.setdefault("concurrency_limit", concurrency_limit)
                retriever_overrides.setdefault(
                    "orchestrator_queue_size", step_outbox_size
                )
                retriever_overrides.setdefault("relationships_only", relationships_only)
                if shard_size_raw is not None:
                    retriever_overrides.setdefault("shard_size", int(shard_size_raw))
            except UnknownTargetError:
                return 1

            logger.info(
                "Starting copy",
                extra={
                    "from": from_target.name,
                    "to": to_target.name,
                    "relationships_only": relationships_only,
                    "node_types": node_types,
                    "relationship_types": rel_types,
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
                    node_types=node_types,
                    relationship_types=rel_types,
                    progress_reporter=reporter,
                    batch_size=batch_size,
                    step_outbox_size=step_outbox_size,
                    flush_concurrency=flush_concurrency,
                    connector_overrides=connector_overrides,
                    retriever_overrides=retriever_overrides,
                )
            )
        return 0

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
        # If the user has specified the target in the options, we don't need to prompt
        # them for anything. We can just use the target they specified.
        if (choice := self.option(action)) is None:
            prompt = f"Which target would you like to copy {action}?"
            choices = [t for t in project.targets_by_name.keys()]
            choice = self.choice(prompt, choices)

        # If the target they specified is unknown, we should error out.
        try:
            return project.get_target_by_name(choice)
        except ValueError:
            self.line_error(f"Unknown target: {choice}")
            raise UnknownTargetError

    def resolve_type_filter(
        self, schema_types: List[GraphObjectSchema], type_name: str
    ) -> List[str]:
        """Return the list of type names to copy for this kind (node/relationship).

        --all (default): all types from schema.
        explicit --node/--relationship flags: validate against schema and use as filter.
        Neither flag set interactively: prompt user to select from schema types.
        """
        all_names = [str(t.name) for t in schema_types]

        if self.option("all"):
            return all_names

        explicit = self.option(type_name)
        if explicit:
            unknown = [t for t in explicit if t not in all_names]
            if unknown:
                self.line_error(
                    f"Unknown {type_name} type(s): {', '.join(unknown)}. "
                    f"Valid options are: {', '.join(all_names)}"
                )
                raise UnknownTargetError
            return list(explicit)

        return self.choice(
            f"Which {type_name} types would you like to copy? (You can select multiple by separating them with a comma)",
            all_names,
            multiple=True,
        )
