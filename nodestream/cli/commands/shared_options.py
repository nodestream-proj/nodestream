from cleo.helpers import argument, option

PROJECT_FILE_OPTION = option(
    "project", "p", "The project file (nodestream.yaml) to load.", flag=False
)

DATABASE_NAME_OPTION = option(
    "database", "db", "The Database to Configre", flag=False, default="neo4j"
)

SCOPE_NAME_OPTION = option(
    "scope", "s", "Show only the pipelines in the provided scope", flag=False
)

JSON_OPTION = option("json", "j", "Log output in JSON", flag=True)


PIPELINE_ARGUMENT = argument("pipeline", "the name of the pipeline")
