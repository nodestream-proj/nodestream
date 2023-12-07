import asyncio
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from typing import Any, AsyncGenerator, Optional

from ..flush import Flush
from ..step import Step


class Transformer(Step):
    """A `Transformer` takes a given record and mutates into a new record.

    `Transformer` steps generally make up the middle of an ETL pipeline and are responsible
    for reshaping an object so its more ingestible by the downstream sink.
    """

    async def handle_async_record_stream(
        self, record_stream: AsyncGenerator[Any, Any]
    ) -> AsyncGenerator[Any, Any]:
        async for record in record_stream:
            if record is Flush:
                yield record
            else:
                val_or_gen = self.transform_record(record)
                if isinstance(val_or_gen, AsyncGenerator):
                    async for result in val_or_gen:
                        yield result
                else:
                    yield await val_or_gen

    @abstractmethod
    async def transform_record(self, record: Any) -> Any:
        raise NotImplementedError


class ConcurrentTransformer(Transformer):
    """A `ConcurrentTransformer` takes a given record and mutates into a new record while managing a pool of concurrent workers.

    `ConcurrentTransformer`s are useful when the transformation work is IO bound and can be parallelized across multiple threads.
    `ConcurrentTransformer`s are not useful when the transformation work is CPU bound as the GIL will prevent useful parallelization.
    Additionally, `ConcurrentTransformer`s do not guarantee the order of the output stream will match the input stream.
    """

    def __init__(
        self, thread_pool_size: Optional[int] = None, maximum_pending_tasks: int = 1000
    ) -> None:
        self.logger = getLogger(self.__class__.__name__)
        self.maximum_pending_tasks = maximum_pending_tasks
        self.thread_pool = ThreadPoolExecutor(
            max_workers=thread_pool_size,
            thread_name_prefix=self.__class__.__name__,
            initializer=self.on_worker_thread_start,
        )

    def on_worker_thread_start(self):
        pass

    async def handle_async_record_stream(
        self, record_stream: AsyncGenerator[Any, Any]
    ) -> AsyncGenerator[Any, Any]:
        pending_tasks = []

        def drain_completed_tasks():
            tasks_drained = 0
            remaining_pending_tasks = []
            for task in pending_tasks:
                if task.done():
                    yield task.result()
                    tasks_drained += 1
                else:
                    remaining_pending_tasks.append(task)

            pending_tasks[:] = remaining_pending_tasks
            if tasks_drained:
                self.logger.debug(
                    "Drained %s completed tasks, %s pending tasks remain",
                    tasks_drained,
                    len(pending_tasks),
                )

        async for record in record_stream:
            if record is Flush:
                for result in drain_completed_tasks():
                    yield result
                yield record
            else:
                submitted = False
                while not submitted:
                    if len(pending_tasks) < self.maximum_pending_tasks:
                        task = self.thread_pool.submit(self.do_work_on_record, record)
                        pending_tasks.append(task)
                        submitted = True
                    for result in drain_completed_tasks():
                        yield result

        self.logger.debug("Finished enqueuing records, draining pending tasks")
        while pending_tasks:
            for result in drain_completed_tasks():
                yield result

    async def finish(self):
        self.thread_pool.shutdown(wait=True)

    def do_work_on_record(self, record: Any) -> Any:
        return asyncio.run(self.transform_record(record))

    @abstractmethod
    async def transform_record(self, record: Any) -> Any:
        raise NotImplementedError
