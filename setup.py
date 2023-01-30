from setuptools import find_packages, setup

setup(
    name="sensor_experiment",
    packages=find_packages(),
    install_requires=[
        "dagster>=1.1.14",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
