from cleo.application import Application

from .commands.run_command import RunCommand

APPLICATION = Application()
APPLICATION.add(RunCommand())


def run():
    APPLICATION.run()


if __name__ == "__main__":
    run()
