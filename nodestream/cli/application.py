from cleo.application import Application

from .commands.new import New
from .commands.run import Run
from .commands.scaffold import Scaffold
from .commands.show import Show

APPLICATION = Application(name="nodestream")
APPLICATION.add(Run())
APPLICATION.add(New())
APPLICATION.add(Show())
APPLICATION.add(Scaffold())


def run():
    APPLICATION.run()


if __name__ == "__main__":
    run()
