import argparse
from pathlib import Path
from subprocess import run
from typing import Optional


BUILD_ARGS = ["uvx", "--from", "build", "pyproject-build", "--installer", "uv"]
PARSER = argparse.ArgumentParser(description="Versioning script")
PARSER.add_argument(
    "--package",
    help="Package name to bump version",
    default="all",
)
PARSER.add_argument(
    "--version",
    help="Version to bump to",
    default=None,
)
PARSER.add_argument(
    "--build",
    action="store_true",
    help="Build package.",
    default=False,
)
PARSER.add_argument(
    "--move-to",
    help="Move the built package to the given directory",
    default=None,
)


class Package:
    def __init__(self, path: Path):
        self.path = path

    @staticmethod
    def all_packages() -> list["Package"]:
        return [Package.nodestream(), *Package.all_plugins()]

    @staticmethod
    def all_plugins() -> list["Package"]:
        return [Package.plugin(d.name) for d in Path.cwd().glob("plugins/*")]

    @staticmethod
    def nodestream() -> "Package":
        return Package(Path.cwd() / "nodestream")

    @staticmethod
    def plugin(name: str) -> "Package":
        return Package(Path.cwd() / "plugins" / name)

    @property
    def project_file(self) -> Path:
        return self.path / "pyproject.toml"

    def set_version(self, version: str) -> str:
        """Set the version of the package to the given version."""
        with self.project_file.open("r") as file:
            lines = file.readlines()
        with self.project_file.open("w") as file:
            lines = [
                f'version = "{version}"\n' if line.startswith("version") else line
                for line in lines
            ]
            file.writelines(lines)

        # Return the tag name that should be used for the version bump
        # This is this way so that the tag name can be used to differentiate
        # which release job should be run in the CI/CD pipeline.
        return f"{self.path.stem}/v{version}"

    def build(self) -> Path:
        """Build the package and return the path to the built package."""
        result = run(BUILD_ARGS, cwd=str(self.path))
        if result.returncode != 0:
            exit(1)
        return self.path / "dist"


def fetch_packages_to_operate_on(package_name: str) -> list[Package]:
    if package_name == "all":
        return Package.all_packages()
    if package_name == "nodestream":
        return [Package.nodestream()]
    return [Package.plugin(package_name)]


def version_package(package: Package, version: str):
    tag_name = package.set_version(version)
    print(f"Version of {package.path.stem} set to {version}")
    print(f"Release Tag name: {tag_name}")


def build_package(package: Package, move_to: Optional[Path]):
    built_package = package.build()
    print(f"Built package: {built_package}")
    if move_to:
        for file in built_package.glob("*"):
            file.rename(move_to / file.name)
        print(f"Moved artfifacts to {move_to}")


def main():
    args = PARSER.parse_args()
    packages_to_operate_on = fetch_packages_to_operate_on(args.package)
    for package in packages_to_operate_on:
        if args.version:
            version_package(package, args.version)
        if args.build:
            move_to = Path(args.move_to) if args.move_to else None
            build_package(package, move_to)


if __name__ == "__main__":
    main()
