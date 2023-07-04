from ...interpreting import Interpreter
from ...schema.indexes import KeyIndex
from ..project import Project
from .audit import Audit


class AuditReferentialIntegrity(Audit):
    name = "refs"
    description = "Audit the correctness of references between nodes of the project"

    async def run(self, project: Project):
        all_interpreters = project.dig_for_step_of_type(Interpreter)
        node_types_so_far = {}

        for definition, step_index, interpreter in all_interpreters:
            for index in interpreter.gather_used_indexes():
                if not isinstance(index, KeyIndex):
                    continue

                if index.type in node_types_so_far:
                    existing = node_types_so_far[index.type]
                    existing_index, existing_definition, existing_step_index = existing
                    if index != existing_index:
                        current_index_fields = ", ".join(index.identity_keys)
                        existing_index_fields = ", ".join(existing_index.identity_keys)
                        self.failure(
                            f"Regarding the type '{index.type}':\n"
                            f"\tthe Interpreter in '{definition.file_path}' step '{step_index}' wants to use an index on the fields '{current_index_fields}' WHILE\n"
                            f"\tthe Interpreter in '{existing_definition.file_path}' step '{existing_step_index}' wants to use an index on the fields '{existing_index_fields}'"
                        )
                else:
                    node_types_so_far[index.type] = (index, definition, step_index)

        if self.failure_count == 0:
            self.success("All Object Types Have Consistent Referential Integrity")
