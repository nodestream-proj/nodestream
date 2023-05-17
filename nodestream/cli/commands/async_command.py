import asyncio

from cleo.commands.command import Command


class AsyncCommand(Command):
    def handle(self):
        asyncio.run(self.handle_async())

    async def handle_async(self):
        ...
