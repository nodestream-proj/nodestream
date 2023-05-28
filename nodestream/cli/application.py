from cleo.application import Application

from .commands.run_command import Run

APPLICATION = Application()
APPLICATION.add(Run())


def run():
    APPLICATION.run()


if __name__ == "__main__":
    run()
