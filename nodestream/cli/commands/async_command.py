import asyncio

from cleo.commands.command import Command
from cleo.io.outputs.output import Verbosity


from ..operations import Operation


class AsyncCommand(Command):
    def handle(self):
        asyncio.run(self.handle_async())

    async def handle_async(self):
        raise NotImplementedError

    async def run_operation(self, opertaion: Operation):
        self.line(
            f"<info>Running: {opertaion.name}</info>", verbosity=Verbosity.VERBOSE
        )
        return await opertaion.perform(self)
