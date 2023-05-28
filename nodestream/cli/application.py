from cleo.application import Application

from .commands.run_command import Run
from .commands.new_project import NewProject

APPLICATION = Application()
APPLICATION.add(Run())
APPLICATION.add(NewProject())


def run():
    APPLICATION.run()


if __name__ == "__main__":
    run()
