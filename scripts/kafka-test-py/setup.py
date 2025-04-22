import os
from setuptools import find_packages, setup

# Force the right package name regardless of directory
os.environ["SETUPTOOLS_INSTALL_PACKAGE_NAME"] = "kafka_test_py"

setup(
    name="kafka_test_py",
    version="0.1.0",
    description="Kafka scenario-based testing tool in Python",
    package_dir={"": "."},  # Add this line
    packages=["core", "scenarios", "utils", "tests"],
    py_modules=["cli"],
    install_requires=[
        "confluent-kafka",
        "pyyaml",
        "click",
        "pytest",
        "pytest-cov",
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
            "black",
            "isort",
            "flake8",
            "mypy",
        ],
    },
    entry_points={
        "console_scripts": [
            "kafka-scenario-test=cli:main",
        ],
    },
    python_requires=">=3.11",
)
