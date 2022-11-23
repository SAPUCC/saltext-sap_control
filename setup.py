# pylint: disable=missing-module-docstring
from pathlib import Path

import setuptools

version_path = Path(__file__).parent / "src" / "saltext" / "sap_control" / "version.py"

with version_path.open() as f:
    for line in f:
        if line.startswith("__version__"):
            # We only want the bare string pls
            line = line.partition("=")[-1].strip()
            line = line.partition("#")[0].strip()
            version = line.strip('"').strip("'")
            break
    else:
        version = "0.0.1dev1"  # pylint: disable=invalid-name

if __name__ == "__main__":
    setuptools.setup(version=version)
