import datetime
from pathlib import Path

from setuptools import find_packages, setup  # type: ignore

root = Path(__file__).parent
req_file = root / "requirements.txt"
# get the dependencies and installs (generated from requirements.in file)
with req_file.open(encoding="utf-8") as f:
    # Strip all comments
    requires = []
    for line in f:
        req = line.split("#", 1)[0].strip()
        if req and not req.startswith("--"):
            requires.append(req)

setup(
    name="data_engineering",
    version=datetime.datetime.utcnow().strftime("%Y%m%d.%H%M%S"),
    description="Data Engineering Package",
    author="DE",
    packages=find_packages(),
    install_requires=requires,
    dependency_links=[
        "https://pypi.org/simple/prefect/",
    ],
    entry_points={
        "console_scripts": [
            "data_flow =data_engineering.core.main_flow:main",
        ],
    },
    package_data={
        "": ["*.yml"],
    },
    include_package_data=True,
    zip_safe=False,
)
