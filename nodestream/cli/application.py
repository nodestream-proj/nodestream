from cleo.application import Application

from .commands.new_project import NewProject
from .commands.run_command import Run

APPLICATION = Application()
APPLICATION.add(Run())
APPLICATION.add(NewProject())


def run():
    APPLICATION.run()


if __name__ == "__main__":
    run()
