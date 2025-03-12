import asyncio
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from typing import Any, AsyncGenerator, Dict, Optional

from ..class_loader import ClassLoader
from ..flush import Flush
from ..step import Step, StepContext
from ..value_providers import ProviderContext, StaticValueOrValueProvider, ValueProvider


class Transformer(Step):
    """A `Transformer` takes a given record and mutates into a new record.

    `Transformer` steps generally make up the middle of an ETL pipeline and
    are responsible for reshaping an object so its more ingestible by the
    downstream sink.
    """

    async def process_record(self, record, _) -> AsyncGenerator[object, None]:
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


class ConcurrentTransformer(Transformer, ABC):
    """A `ConcurrentTransformer` can process multiple records concurrently.

    `ConcurrentTransformer`s are useful when the transformation work is IO
    bound and can be parallelized across multiple threads.
    `ConcurrentTransformer`s are not useful when the transformation work is
    CPU bound as the GIL will prevent useful parallelization. Additionally,
    `ConcurrentTransformer`s do not guarantee the order of the output stream
    will match the input stream.
    """

    def __init__(
        self,
        thread_pool_size: Optional[int] = None,
        maximum_pending_tasks: int = 1000,
    ) -> None:
        self.logger = getLogger(self.__class__.__name__)
        self.maximum_pending_tasks = maximum_pending_tasks
        self.thread_pool = ThreadPoolExecutor(
            max_workers=thread_pool_size,
            thread_name_prefix=self.__class__.__name__,
            initializer=self.on_worker_thread_start,
        )
        self.pending_tasks = []

    def on_worker_thread_start(self):
        pass  # do nothing

    async def drain_completed_tasks(self):
        tasks_drained = 0
        remaining_pending_tasks = []
        for task in self.pending_tasks:
            if task.done():
                yield task.result()
                tasks_drained += 1
            else:
                remaining_pending_tasks.append(task)

        await self.yield_processor()

        self.pending_tasks[:] = remaining_pending_tasks
        if tasks_drained:
            self.logger.debug(
                "Drained %s completed tasks, %s pending tasks remain",
                tasks_drained,
                len(self.pending_tasks),
            )

    async def process_record(self, record: Any, _) -> Any:
        if record is Flush:
            async for outstanding in self.drain_completed_tasks():
                yield outstanding
            yield record
        else:
            # Submit the work to the thread pool (only if we have capacity)
            # If we don't have capacity, yield completed tasks.
            # Once there is nothing remaining to yield from these tasks,
            # yield the processor until capacity is available.
            submitted = False
            while not submitted:
                if len(self.pending_tasks) < self.maximum_pending_tasks:
                    task = self.thread_pool.submit(self.do_work_on_record, record)
                    self.pending_tasks.append(task)
                    submitted = True
                async for result in self.drain_completed_tasks():
                    yield result

    def do_work_on_record(self, record: Any) -> Any:
        return asyncio.run(self.transform_record(record))

    async def yield_processor(self):
        await asyncio.sleep(0)

    async def emit_outstanding_records(self, context: StepContext):
        while self.pending_tasks:
            async for result in self.drain_completed_tasks():
                yield result

    async def finish(self, _: StepContext):
        await asyncio.get_event_loop().run_in_executor(
            None, self.thread_pool.shutdown, True
        )


class PassTransformer(Transformer):
    async def transform_record(self, record: Any) -> AsyncGenerator:
        yield record


class SwitchTransformer(Transformer):
    __slots__ = (
        "switch_on",
        "transformers",
        "default",
        "normalization",
    )

    @classmethod
    def from_file_data(cls, switch_on, cases, default=None, normalization=None):
        loader = ClassLoader.instance(Step)
        transformers = {
            field_value: loader.load_class(**transformer)
            for field_value, transformer in cases.items()
        }
        default = loader.load_class(**default) if default else None
        return cls(switch_on, transformers, default, normalization)

    def __init__(
        self,
        switch_on: StaticValueOrValueProvider,
        transformers: Dict[str, Transformer],
        default: Transformer = None,
        normalization: Dict[str, Any] = None,
    ):
        self.switch_on = ValueProvider.guarantee_value_provider(switch_on)
        self.transformers = transformers
        self.default = default if default else PassTransformer()
        self.normalization = normalization or {}

    async def transform_record(self, record: Any):
        context = ProviderContext.fresh(record)
        key = self.switch_on.normalize_single_value(context, self.normalization)
        transformer = self.transformers.get(key, self.default)
        # If we don't have a transformer to handle the record we just yield
        # the record with PassTransformer
        async for result in transformer.transform_record(record):
            yield result
