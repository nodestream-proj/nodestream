"""Generate the code reference pages."""

from pathlib import Path

import mkdocs_gen_files

for path in sorted(Path("nodestream").rglob("*.py")):  #
    module_path = path.relative_to("nodestream").with_suffix("")  #
    doc_path = path.relative_to("nodestream").with_suffix(".md")  #
    full_doc_path = Path("python_reference", doc_path)  #

    parts = ["nodestream", *module_path.parts]
    if parts[-1] in ["__main__", "__init__"]:
        continue

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:  #
        identifier = ".".join(parts)  #
        print("::: " + identifier, file=fd)  #

    mkdocs_gen_files.set_edit_path(full_doc_path, path)  #
