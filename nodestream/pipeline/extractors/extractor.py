from abc import abstractmethod
from typing import Any, AsyncGenerator, Generic, TypeVar

from ..step import Step, StepContext

R = TypeVar("R")
T = TypeVar("T")
CHECKPOINT_OBJECT_KEY = "extractor_progress_checkpoint"


class Extractor(Step, Generic[R, T]):
    """Extractors represent the source of a set of records.

    They are like any other step. However, they ignore the incoming record '
    stream and instead produce their own stream of records. For this reason
    they generally should only be set at the beginning of a pipeline.
    """

    CHECKPOINT_INTERVAL = 1000

    async def start(self, context: StepContext):
        if checkpoint := context.object_store.get_pickled(CHECKPOINT_OBJECT_KEY):
            context.info("Found Checkpoint For Extractor. Signaling to resume from it.")
            await self.resume_from_checkpoint(checkpoint)

    async def finish(self, context: StepContext):
        if not context.pipeline_encountered_fatal_error:
            context.debug(
                "Clearing checkpoint for extractor since extractor is finished."
            )
            context.object_store.delete(CHECKPOINT_OBJECT_KEY)

    async def make_checkpoint(self) -> T:
        return None

    async def resume_from_checkpoint(self, checkpoint_object: T):
        pass

    async def commit_checkpoint(self, context: StepContext) -> None:
        if checkpoint := await self.make_checkpoint():
            context.object_store.put_picklable(CHECKPOINT_OBJECT_KEY, checkpoint)

    async def emit_outstanding_records(
        self, context: StepContext
    ) -> AsyncGenerator[R, None]:
        items_generated = 0
        async for record in self.extract_records():
            yield record
            items_generated += 1
            if items_generated % self.CHECKPOINT_INTERVAL == 0:
                await self.commit_checkpoint(context)

    @abstractmethod
    def extract_records(self) -> AsyncGenerator[R, Any]:
        raise NotImplementedError
