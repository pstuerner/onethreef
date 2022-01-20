from setuptools import setup

setup(
    name="onethreef",
    version="0.1",
    packages=['onethreef'],
    entry_points={
        "console_scripts": [
            "onethreef = onethreef.__main__:main"
        ]
    }
)
