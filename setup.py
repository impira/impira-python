import os
import setuptools

dir_name = os.path.abspath(os.path.dirname(__file__))

version_contents = {}
with open(os.path.join(dir_name, "src", "impira", "version.py"), encoding="utf-8") as f:
    exec(f.read(), version_contents)

with open(os.path.join(dir_name, "README.md"), "r", encoding="utf-8") as f:
    long_description = f.read()

setuptools.setup(
    name="impira",
    version=version_contents['VERSION'],
    author="Impira Engineering",
    author_email="engineering@impira.com",
    description="Official Impira Python SDK",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/impira/impira-python",
    project_urls={
        "Bug Tracker": "https://github.com/impira/impira-python/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
        'pydantic',
        'requests',
        'typing',
    ]
)
