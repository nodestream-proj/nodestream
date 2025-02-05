import pytest

from hamcrest import assert_that, is_, none

from nodestream.pipeline import Extractor
from nodestream.pipeline.step import StepContext
from nodestream.pipeline.progress_reporter import PipelineProgressReporter
from nodestream.pipeline.object_storage import ObjectStore



class DummyExtractor(Extractor):
    def __init__(self, checkpoint = None):
        self.checkpoint = checkpoint

    async def extract_records(self):
        for x in []:
            yield x

    async def make_checkpoint(self):
        return self.checkpoint

    async def resume_from_checkpoint(self, checkpoint_object):
        self.checkpoint = checkpoint_object

@pytest.fixture
def object_store(mocker):
    return mocker.Mock(spec=ObjectStore)

@pytest.fixture
def context(object_store):
    return  StepContext("test", 1, PipelineProgressReporter(), object_store)

@pytest.mark.asyncio
async def test_extractor_commit_does_nothing(object_store, context):
    extractor = DummyExtractor()
    await extractor.commit_checkpoint(context)
    object_store.put_picklable.assert_not_called()


@pytest.mark.asyncio
async def test_extractor_commit_does_something_with_checkpoint(object_store, context):
    extractor = DummyExtractor(checkpoint=1)
    await extractor.commit_checkpoint(context)
    object_store.put_picklable.assert_called_once_with("extractor_progress_checkpoint", 1)


@pytest.mark.asyncio
async def test_extractor_finish_cleans(object_store, context):
    extractor = DummyExtractor()
    await extractor.finish(context)
    object_store.delete.assert_called_once_with("extractor_progress_checkpoint")


@pytest.mark.asyncio
async def test_extractor_start_does_nothing_when_no_checkpoint(object_store, context):
    extractor = DummyExtractor()
    object_store.get_pickled.return_value = None
    await extractor.start(context)
    assert_that(extractor.checkpoint, is_(none()))


@pytest.mark.asyncio
async def test_extractor_start_does_something_when_checkpoint(object_store, context):
    extractor = DummyExtractor()
    object_store.get_pickled.return_value = 1
    await extractor.start(context)
    assert_that(extractor.checkpoint, is_(1))
