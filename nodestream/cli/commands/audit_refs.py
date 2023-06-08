from ...audits import AuditReferencialIntegrity
from .audit_command import AuditCommand


class AuditRefs(AuditCommand):
    name = "audit refs"
    description = "Audit the correctness of references between nodes of the project"
    audit = AuditReferencialIntegrity
