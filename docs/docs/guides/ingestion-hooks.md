# Ingestion Hooks

Ingestion Hooks provide a way to execute additional queries when running an ingest. 
Ingestion Hooks can are executed either _before_ or _after_ all the data in a batch is ingested.
Each hook is executed in its own transaction and executed in the order they are added.
Ingestion hooks are database specific because you have to write the queries yourself. 
Therefore, you must be sure to implement the hooks for each database you support. 

!!! caution

    Ingestion hooks are an advanced feature. 
    Likely there is other ways to do the same work that introduce less complexity.
    They are considered an escape hatch for when you need to do something that is not supported by the framework.


## Creating an Ingestion Hook

Ingestion hooks are created by extending the `IngestionHook` class and implementing a method such as the `as_cypher_query_and_parameters` method. This method returns a tuple of the query and the parameters to be used in the query. The query is executed in a transaction. The parameters are passed to the query as parameters. 

```python
from dataclasses import dataclass

from nodestream.model import IngestionHook

CLEAN_UP_QUERY = "MATCH (o:Order) where o.id < $id DETACH DELETE o"

@dataclass
class DeleteOrder(IngestionHook):
    id: str

    def as_cypher_query_and_parameters(self):
        return CLEAN_UP_QUERY, {"id": self.id}
```

## Triggering an Ingestion Hook

Ingestion hooks have to added at some point. 
This is generally done as part of a [custom interpretation](./creating-your-own-interpretation.md).

For example, if you wanted to delete an order, you could create a custom interpretation that adds the hook to the `DesiredIngestion` object.

```python
from nodestream.model import DesiredIngestion
from nodestream.pipeline.value_providers import ProviderContext


class DeleteOrderIntepretation(Interpretation, alias="delete_order"):
    # __init__ omitted 

    def interpret(self, context: ProviderContext):
        hook = DeleteOrder(id=self.order_id)
        context.desired_ingest.add_ingest_hook(hook, before_ingest=True) 
```
