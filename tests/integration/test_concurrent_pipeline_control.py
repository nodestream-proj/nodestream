import asyncio
from typing import Any, AsyncGenerator, List

import pytest
from hamcrest import contains_inanyorder

from nodestream.pipeline.extractors import IterableExtractor
from nodestream.pipeline.pipeline import Pipeline, Step, StepExecutor
from tests.unit.pipeline.transformers.test_transformer import AddOneConcurrently


class AddOneConcurrentlyGreedy(AddOneConcurrently):
    async def yield_processor(self):
        pass


"""
This is the main testing suite for the pipeline. Here is what happens:
1. We initialize this test with the upstream step so that we have visibility on the status of the outbox (Queue)
2. Once the upstream client has yielded the processor, we will enter handle_async_record_stream for the first time
    We then empty the contents of the queue into our test box until the upstream's box is empty. 
3. Once the upstream's box is empty we yield the processor back to the event loop. 

If we are only visited once, then the transformer was greedy and we're only visited once. 
Otherwise we would expect the outbox to obtain more contents as we get into and out of this step.
"""


class MockStep(Step):
    def __init__(self):
        self.box_list = []
        self.times_visited = 1
        self.upstream_executor = None
        self.results = []

    def link_to_upstream_step(self, upstream_executor: StepExecutor):
        self.upstream_executor = upstream_executor

    # Here is where we test to see what is happening to the box
    async def handle_async_record_stream(self, record_stream: AsyncGenerator[Any, Any]):
        # We exit if the upstream step is done AND there's nothing in the box. (Demorgans law)
        while (
            not self.upstream_executor.done or not self.upstream_executor.outbox.empty()
        ):
            async for record in record_stream:
                self.box_list.append(record)
                yield record
                # If there is nothing left in the box, we may leave.
                if self.upstream_executor.outbox.empty():
                    break
            self.results.append(
                {"times_visited": self.times_visited, "box_contents": self.box_list}
            )
            # Increment the times_visited and yield the processor.
            await self.yield_processor()

    async def yield_processor(self):
        self.times_visited += 1
        await asyncio.sleep(0)


"""
This pipeline's sole purpose is to allow me to visit an upstream's outbox queue directly for testing purposes. 
"""


class TestPipeline(Pipeline):
    def __init__(self, steps: List[Step], step_outbox_size: int) -> None:
        self.steps = steps
        self.step_outbox_size = step_outbox_size
        self.errors = []
        self.executors = []
        self.tasks = self.build_steps_into_tasks()

    # modified to keep the executors accesible from the pipeline
    def build_steps_into_tasks(
        self,
    ):
        current_executor = None
        tasks = []

        for step_index, step in enumerate(self.steps):
            current_executor = StepExecutor(
                upstream=current_executor,
                step=step,
                outbox_size=self.step_outbox_size,
                step_index=step_index,
            )
            self.executors.append(current_executor)
            tasks.append(asyncio.create_task(current_executor.work_loop()))
        return tasks

    # modified since tasks are built from init
    async def run(
        self,
    ) -> AsyncGenerator[Any, Any]:
        return_states = await asyncio.gather(*self.tasks, return_exceptions=True)
        self.propogate_errors_from_return_states(return_states)


""" 
Test that the concurrent transformer yields the processor
In this test we will have the following:
1. IterableExtractor (provides range(5))
2. AddOneConcurrentlyModified
3. MockStep 

Upon pipeline intiialization:
1. IteratableExtractor provides [0-4] between AddOneConcurrentlyModified
2. AddOneConcurrentlyModified submits 2 pending tasks to the task list
3. On the 3'rd task it will attempt to drain the results
4. After the drain the processor should be yielded.
5. The MockStep will be initialized and we will immediately test for ONLY 1 or ONLY (1,2)
"""


@pytest.mark.asyncio
async def test_concurrent_transformer_yields_processor():
    writer = MockStep()
    pipeline = TestPipeline(
        [
            IterableExtractor(range(20)),
            AddOneConcurrently(maximum_pending_tasks=10, thread_pool_size=2),
            writer,
        ],
        100,
    )
    writer.link_to_upstream_step(pipeline.executors[1])
    await pipeline.run()
    print(writer.results)
    assert len(writer.results) > 1
    assert contains_inanyorder(writer.results[-1]["box_contents"], list(range(1, 21)))


@pytest.mark.asyncio
async def test_greedy_concurrent_transformer_does_not_yield_processor():
    writer = MockStep()
    pipeline = TestPipeline(
        [
            IterableExtractor(range(20)),
            AddOneConcurrentlyGreedy(maximum_pending_tasks=10, thread_pool_size=2),
            writer,
        ],
        100,
    )
    writer.link_to_upstream_step(pipeline.executors[1])
    await pipeline.run()
    assert len(writer.results) == 1
    assert contains_inanyorder(writer.results[0]["box_contents"], list(range(1, 21)))
