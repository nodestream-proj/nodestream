import importlib.metadata
import os
import sys

from cleo.application import Application

from .commands.audit_refs import AuditRefs
from .commands.audit_ttls import AuditTTLs
from .commands.new import New
from .commands.print_schema import PrintSchema
from .commands.remove import Remove
from .commands.run import Run
from .commands.scaffold import Scaffold
from .commands.show import Show

APPLICATION = Application(
    name="nodestream", version=importlib.metadata.version("nodestream")
)
APPLICATION.add(Run())
APPLICATION.add(New())
APPLICATION.add(Show())
APPLICATION.add(Scaffold())
APPLICATION.add(Remove())
APPLICATION.add(PrintSchema())
APPLICATION.add(AuditTTLs())
APPLICATION.add(AuditRefs())


def run():
    # For installed users, the current working directory is not guaranteed to be
    # in the python path. So, we will guarantee it is by adding it to path before we
    # run the application.
    sys.path.append(os.getcwd())
    APPLICATION.run()


if __name__ == "__main__":
    run()
