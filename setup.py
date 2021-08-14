import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="impira",
    version="0.0.2",
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
        'json',
        'pydantic',
        'requests',
        'typing',
        'urllib',
    ]
)
