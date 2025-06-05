import asyncio
from asyncio import Queue, wait_for
from typing import Optional, Tuple

from ..metrics import Metric, Metrics


#  WARNING: DoneObject refernces should not exist beyond this module.
class DoneObject:
    """A `DoneObject` is a marker object that indicates that a step is done.

    When a step is done processing records, it should emit a `DoneObject` to
    indicate that it is finished. This allows downstream steps to know when a
    step is done and they can stop processing records.

    The `DoneObject` is a special object that is used to signal the end of a
    stream of records. It is not a record itself and should not be processed as
    such.
    """

    pass


CHANNEL_TIMEOUT = 0.25


class Channel:
    """`Channel` is a communication channel between steps in a pipeline.

    A `Channel` is used to pass records between steps in a pipeline. It is a
    simple queue that can be used to pass records from one step to another. The
    `Channel` is asynchronous and can be used to pass records between steps in
    an asynchronous context.

    A `Channel` has a fixed size and will block if the queue is full. This
    allows the pipeline to control the flow of records between steps and
    prevent one step from overwhelming another step with too many records.
    """

    __slots__ = ("queue", "input_dropped", "metric")

    def __init__(self, size: int, metric: Metric) -> None:
        self.queue = Queue(maxsize=size)
        self.input_dropped = False
        self.metric = metric

    @classmethod
    def create_with_naming(
        cls, size: int, input_name: str = "Void", output_name: str = "Void"
    ) -> "Channel":
        metric = Metric(
            f"buffered_{input_name}_to_{output_name}",
            f"Records buffered: {input_name} → {output_name}",
        )
        return cls(size, metric)

    async def get(self):
        """Get an object from the channel.

        This method is used to get an object from the channel. It will block
        until an object is available in the channel. If the channel is empty,
        it will block until an object is available.

        Returns:
            object: The object that was retrieved from the channel.
        """
        object = await self.queue.get()
        Metrics.get().decrement(self.metric)
        return object

    async def put(self, obj) -> bool:
        """Put an object in the channel.

        This method is used to put an object in the channel. It will block
        until the object is put in the channel. If the channel is full, it
        will block until there is space in the channel.

        Returns:
            bool: True if the object was successfully put in the channel, False
            if the channel is full and the object was not put in the channel.
        """
        try:
            await wait_for(self.queue.put(obj), timeout=CHANNEL_TIMEOUT)
            Metrics.get().increment(self.metric)
            return True
        except (TimeoutError, asyncio.TimeoutError):
            return False


class StepOutput:
    """`StepOutput` is an output channel for a step in a pipeline.

    A `StepOutput` is used to pass records from a step to the next step in a
    pipeline. It is a simple wrapper around a `Channel` that provides a more
    convenient interface for putting records in the channel.
    """

    __slots__ = ("channel",)

    def __init__(self, channel: Channel) -> None:
        self.channel = channel

    async def done(self):
        """Mark the output channel as done.

        This method is used to mark the output channel as done. It will put a
        `DoneObject` in the channel to indicate that the step is finished
        processing records. This allows downstream steps to know when the step
        is done and they can stop processing records.
        """
        await self.put(DoneObject)

    async def put(self, obj) -> bool:
        """Put an object in the output channel.

        This method is used to put an object in the output channel. It will
        block until the object is put in the channel. If the channel is full,
        it will block until there is space in the channel unless the channel is
        closed on the other end.

        Returns:
            bool: True if the object was successfully put in the channel, False
            if the channel is closed on the other end and the object was not
            put in the channel.
        """
        successfully_put = False
        while not successfully_put and not self.channel.input_dropped:
            successfully_put = await self.channel.put(obj)

        return successfully_put


class StepInput:
    """`StepInput` is an input channel for a step in a pipeline.

    A `StepInput` is used to get records from the previous step in a pipeline.
    It is a simple wrapper around a `Channel` that provides a more convenient
    interface for getting records from the channel.

    The `StepInput` is an asynchronous generator that can be used to get
    records from the channel in an asynchronous context.
    """

    __slots__ = ("channel",)

    def __init__(self, channel: Channel) -> None:
        self.channel = channel

    async def get(self) -> Optional[object]:
        """Get an object from the input channel.

        This method is used to get an object from the input channel. It will
        block until an object is available in the channel. Once the channel is
        closed (i.e. a `DoneObject` is received), this method will return None.

        Returns:
            object: The object that was retrieved from the channel.
        """
        if (object := await self.channel.get()) is DoneObject:
            return None

        return object

    def done(self):
        """Mark the input channel as done.

        This will close the channel.
        """
        self.channel.input_dropped = True


def channel(
    size: int, input_name: str | None = None, output_name: str | None = None
) -> Tuple[StepInput, StepOutput]:
    """Create a new input and output channel.

    Args:
        size: The size of the channel.
        input_name: The name of the input step.
        output_name: The name of the output step.
    """
    channel = Channel.create_with_naming(size, input_name, output_name)
    return StepInput(channel), StepOutput(channel)
