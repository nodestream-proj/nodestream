import importlib.metadata
import os
import sys

from cleo.application import Application

from ..project.audits import Audit
from .commands import AuditCommand, NodestreamCommand

PACKAGE_NAME = "nodestream"


def get_version() -> str:
    return importlib.metadata.version(PACKAGE_NAME)


def get_application() -> Application:
    app = Application(PACKAGE_NAME, get_version())
    for audit in Audit.all():
        app.add(AuditCommand.for_audit(audit)())
    for command in NodestreamCommand.all():
        if command not in (AuditCommand, NodestreamCommand):
            app.add(command())
    return app


def run():
    sys.path.append(os.getcwd())
    get_application().run()
