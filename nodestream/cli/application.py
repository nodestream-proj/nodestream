import os
import sys

from cleo.application import Application

from .commands.new import New
from .commands.remove import Remove
from .commands.run import Run
from .commands.scaffold import Scaffold
from .commands.show import Show

APPLICATION = Application(name="nodestream")
APPLICATION.add(Run())
APPLICATION.add(New())
APPLICATION.add(Show())
APPLICATION.add(Scaffold())
APPLICATION.add(Remove())


def run():
    # For installed users, the current working directory is not garunteed to be
    # in the python path. So, we will garuntee it is by adding it to path before we
    # run the application.
    sys.path.append(os.getcwd())
    APPLICATION.run()


if __name__ == "__main__":
    run()
