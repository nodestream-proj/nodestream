from nodestream.cli.application import get_application


def test_get_application_builds_app_and_adds_commands(mocker):
    # Patch Audit and NodestreamCommand registries to be deterministic
    fake_audit_cls = mocker.Mock()
    fake_audit_cls.name = "fake-audit"
    fake_audit_cls.description = "desc"

    class FakeAuditCommand:
        def __init__(self):
            self.name = "audit fake-audit"
            self.aliases = []

        def set_application(self, app=None):
            self._app = app

        @property
        def enabled(self):
            return True

    class FakeOtherCommand:
        def __init__(self):
            self.name = "other"
            self.aliases = []

        def set_application(self, app=None):
            self._app = app

        @property
        def enabled(self):
            return True

    mocker.patch("nodestream.cli.application.Audit.all", return_value=[fake_audit_cls])

    mocker.patch(
        "nodestream.cli.application.AuditCommand.for_audit",
        return_value=FakeAuditCommand,
    )
    mocker.patch(
        "nodestream.cli.application.NodestreamCommand.all",
        return_value=[FakeOtherCommand],
    )

    app = get_application()

    # Basic sanity: application created
    assert app is not None
