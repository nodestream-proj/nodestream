import importlib.metadata
import os
import sys

from cleo.application import Application

from ..project.audits import Audit
from .commands import AuditCommand, NodestreamCommand

PACKAGE_NAME = "nodestream"


def get_application() -> Application:
    app = Application(PACKAGE_NAME, importlib.metadata.version(PACKAGE_NAME))
    for audit in Audit.all():
        app.add(AuditCommand.for_audit(audit)())
    for command in NodestreamCommand.all():
        app.add(command())
    return app


def run():
    sys.path.append(os.getcwd())
    get_application().run()
