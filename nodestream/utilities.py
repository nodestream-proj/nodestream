import platform
import resource
from pathlib import Path
from typing import Any

import yaml


async def enumerate_async(iterable):
    count = 0

    async for item in iterable:
        yield count, item
        count += 1


def get_max_mem_mb():
    max_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    max_mem /= 1024
    if platform.system() == "Darwin":
        max_mem /= 1024
    return int(max_mem)


def pretty_print_yaml_to_file(path: Path, data: Any):
    with open(path, "w") as fp:
        yaml.safe_dump(data, fp, indent=2, sort_keys=True)
