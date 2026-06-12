import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

version = {}
with open("pytapo/version.py", "r") as fh:
    exec(fh.read(), version)

setuptools.setup(
    name="pytapo",
    version=version["PYTAPO_VERSION"],
    author="Juraj Nyíri",
    author_email="juraj.nyiri@gmail.com",
    description="Python library for communication with Tapo Cameras",
    license="MIT",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/JurajNyiri/pytapo",
    packages=setuptools.find_packages(),
    python_requires=">=3.11",
    install_requires=[
        "requests",
        "urllib3",
        "pycryptodome",
        "rtp",
        "python-kasa",
        "aiofiles",
    ],
    tests_require=["pytest", "pytest-asyncio", "mock"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
