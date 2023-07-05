import os
import sys
import importlib.metadata

from cleo.application import Application

from .commands import NodestreamCommand, AuditCommand
from ..project.audits import Audit

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
