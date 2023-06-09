from ...audits import TTLAudit
from .audit_command import AuditCommand


class AuditTTLs(AuditCommand):
    name = "audit ttls"
    description = "Audit the project for missing TTLs"
    audit = TTLAudit
