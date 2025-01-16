from cleo.helpers import argument, option

PROJECT_FILE_OPTION = option(
    "project", "p", "The project file (nodestream.yaml) to load.", flag=False
)

DATABASE_NAME_OPTION = option(
    "database", "d", "The Database to Configure", flag=False, default="neo4j"
)

SCOPE_NAME_OPTION = option(
    "scope", "s", "Use the provided scope for the operation", flag=False
)

JSON_OPTION = option("json", "j", "Log output in JSON", flag=True)


PIPELINE_ARGUMENT = argument("pipeline", "the name of the pipeline")
MANY_PIPELINES_ARGUMENT = argument(
    "pipelines", "the names of the pipelines", multiple=True, optional=True
)

TARGETS_OPTION = option(
    "target",
    "t",
    "Specify a database to target at run time.",
    multiple=True,
    flag=False,
)

PROMETHEUS_OPTION = option(
    "prometheus",
    description="Enable the Prometheus metrics server",
    flag=True,
)

PROMETHEUS_PORT_OPTION = option(
    "prometheus-listen-port",
    description="The port to run the Prometheus metrics server on",
    flag=False,
    default="9090",
)

PROMETHEUS_ADDRESS_OPTION = option(
    "prometheus-listen-address",
    description="The address to run the Prometheus metrics server on",
    flag=False,
    default="0.0.0.0",
)

PROMETHEUS_CERTFILE_OPTION = option(
    "prometheus-certfile",
    description="The path to the certificate file for the Prometheus metrics server",
    flag=False,
)

PROMETHEUS_KEYFILE_OPTION = option(
    "prometheus-keyfile",
    description="The path to the key file for the Prometheus metrics server",
    flag=False,
)


PROMETHEUS_OPTIONS = [
    PROMETHEUS_OPTION,
    PROMETHEUS_PORT_OPTION,
    PROMETHEUS_ADDRESS_OPTION,
    PROMETHEUS_CERTFILE_OPTION,
    PROMETHEUS_KEYFILE_OPTION,
]
