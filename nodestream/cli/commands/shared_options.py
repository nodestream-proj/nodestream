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
