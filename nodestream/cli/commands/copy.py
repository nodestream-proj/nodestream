from typing import Dict, List

from cleo.helpers import option

from ...metrics import Metrics
from ...pipeline import PipelineProgressReporter
from ...project import Project, Target
from ...schema import GraphObjectSchema
from ..operations import (
    InitializeLogger,
    InitializeMetricsHandler,
    InitializeProject,
    RunCopy,
)
from ..operations.run_pipeline import (
    JsonProgressIndicator,
    SpinnerProgressIndicator,
)
from .nodestream_command import NodestreamCommand
from .shared_options import JSON_OPTION, PROJECT_FILE_OPTION, PROMETHEUS_OPTIONS


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
        option("run-concurrently", "r", "Run the copy concurrently", flag=True),
        option(
            "concurrency-limit",
            "c",
            "The concurrency limit for the copy",
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
            "storage-backend",
            description="Storage backend to use for checkpointing",
            flag=False,
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
                # For copy operations we intentionally build a schema *without*
                # expanding additional types so that we only copy the concrete
                # node / relationship types declared in pipelines.
                schema = project.make_schema(include_additional_types=False)
                all_node_types = schema.nodes
                all_rel_types = schema.relationships
                node_types = self.get_type_selection_from_user(all_node_types, "node")
                rel_types = self.get_type_selection_from_user(
                    all_rel_types, "relationship"
                )
                run_concurrently = self.option("run-concurrently")
                concurrency_limit = int(self.option("concurrency-limit") or 10)
                batch_size = int(self.option("batch-size"))
                step_outbox_size = int(self.option("step-outbox-size"))
                flush_concurrency = int(self.option("flush-concurrency"))
                connector_overrides = self.parse_key_value_options("connector-option")
                retriever_overrides = self.parse_key_value_options("retriever-option")
                storage_name = self.option("storage-backend")
                object_store = project.get_object_storage_by_name(storage_name)
            except UnknownTargetError:
                return

            self.line("Starting to Copy:")
            self.line(f"<info>From: {from_target.name}</info>")
            self.line(f"<info>To: {to_target.name}</info>")
            self.line(f"<info>Node Types: {', '.join(node_types)}</info>")
            self.line(f"<info>Relationship Types: {', '.join(rel_types)}</info>")
            self.line(f"<info>Batch Size: {batch_size}</info>")
            self.line(f"<info>Step Outbox Size: {step_outbox_size}</info>")
            self.line(f"<info>Flush Concurrency: {flush_concurrency}</info>")
            self.line(f"<info>Storage Backend: {storage_name or 'none'}</info>")
            if connector_overrides:
                self.line(f"<info>Connector Overrides: {connector_overrides}</info>")
            if retriever_overrides:
                self.line(f"<info>Retriever Options: {retriever_overrides}</info>")
            reporter = self.create_progress_reporter()
            await self.run_operation(
                RunCopy(
                    from_target=from_target,
                    to_target=to_target,
                    schema=schema,
                    node_types=node_types,
                    relationship_types=rel_types,
                    run_concurrently=run_concurrently,
                    concurrency_limit=concurrency_limit,
                    progress_reporter=reporter,
                    batch_size=batch_size,
                    step_outbox_size=step_outbox_size,
                    flush_concurrency=flush_concurrency,
                    connector_overrides=connector_overrides,
                    retriever_overrides=retriever_overrides,
                    object_store=object_store,
                )
            )

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

    def create_progress_reporter(self) -> PipelineProgressReporter:
        if self.has_json_logging_set:
            indicator = JsonProgressIndicator(self, "copy")
        else:
            indicator = SpinnerProgressIndicator(self, "copy")

        metrics_interval_in_seconds = (
            float(self.option("metrics-interval-in-seconds"))
            if self.option("metrics-interval-in-seconds")
            else None
        )

        return PipelineProgressReporter(
            reporting_frequency=int(self.option("reporting-frequency")),
            metrics_interval_in_seconds=metrics_interval_in_seconds,
            callback=indicator.progress_callback,
            on_start_callback=indicator.on_start,
            on_finish_callback=indicator.on_finish,
            on_fatal_error_callback=indicator.on_fatal_error,
        )

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

    def get_type_selection_from_user(
        self, types: List[GraphObjectSchema], type_name: str
    ) -> List[str]:
        choices = [str(t.name) for t in types]

        # If the user has specified the --all flag, we don't need to prompt them for
        # anything. We can just return all the types.
        if self.option("all"):
            return choices

        # If the user has specified type(s) in the options, we don't need to prompt
        # them for anything. We can just return the type they specified. If they
        # specified an unknown type, we should error out.
        selections_from_options = self.option(type_name)
        if selections_from_options:
            for selection in selections_from_options:
                if selection not in choices:
                    self.line_error(
                        f"Unknown {type_name} type: {selection}. "
                        f"Valid options are: {', '.join(choices)}"
                    )
                    raise UnknownTargetError
            return selections_from_options

        # If the user has not specified the type(s) in the options, we need to prompt
        # them for the type(s). We can just return the type they specified.
        return self.choice(
            f"Which {type_name} types would you like to copy? (You can select multiple by separating them with a comma)",
            choices,
            multiple=True,
        )
