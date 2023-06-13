from ...audits import AuditReferentialIntegrity
from .audit_command import AuditCommand


class AuditRefs(AuditCommand):
    name = "audit refs"
    description = "Audit the correctness of references between nodes of the project"
    audit = AuditReferentialIntegrity
