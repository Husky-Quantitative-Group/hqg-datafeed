# Publishing hqg-datafeed

Run commands from the `hqg-datafeed` repository root. The project uses
setuptools with metadata in `pyproject.toml`, and retains the public
`datafeed` import package:

```text
hqg-datafeed/
├── pyproject.toml
├── MANIFEST.in
├── README.md
├── datafeed/
│   ├── __init__.py
│   ├── altdata/
│   └── securities/
└── docs/
    └── publishing.md
```

## Prepare a release

1. Set an unused release version in `pyproject.toml` (currently `0.1.0`).
   PyPI does not allow replacing previously uploaded distribution files.
2. Choose the project's license before the first public release. Add a
   `LICENSE` file and the matching `[project]` license metadata. No license
   has been selected in this repository.
3. Create a virtual environment and install the development tools:

   ```shell
   python -m venv .venv
   source .venv/bin/activate
   python -m pip install --upgrade pip
   python -m pip install -e ".[test,release]"
   python -m pytest
   ```

## Build and validate

Start with an empty `dist/` directory so older releases cannot be uploaded
accidentally. Build the source distribution and wheel, then check their metadata
and README rendering:

```shell
python -m build
python -m twine check --strict dist/*
```

For version `0.1.0`, the files are:

```text
dist/hqg_datafeed-0.1.0.tar.gz
dist/hqg_datafeed-0.1.0-py3-none-any.whl
```

The wheel contains `datafeed` and its subpackages. The source distribution
also includes the README, build configuration, and this guide. Local tests,
credentials, virtual environments, and build output are excluded.

Verify the wheel from outside the checkout in a fresh virtual environment:

```shell
python -m venv /tmp/hqg-datafeed-release-check
/tmp/hqg-datafeed-release-check/bin/python -m pip install "$PWD/dist/hqg_datafeed-0.1.0-py3-none-any.whl"
cd /tmp
/tmp/hqg-datafeed-release-check/bin/python -c 'from datafeed import AltDataProvider, Config, DataFeed, DataFeedError, MarketData; DataFeed(Config())'
```

Return to the repository root before uploading. Adjust the filenames when
releasing a different version.

## Upload

Optionally upload to TestPyPI first:

```shell
python -m twine upload --repository testpypi dist/*
```

Upload the validated artifacts to PyPI:

```shell
python -m twine upload dist/*
```

Use `__token__` as the username and your PyPI API token as the password when
prompted. TestPyPI uses a separate account and token. Keep tokens out of
source control. For an existing project, your account needs publishing access;
for a first release, the distribution name must be available to your account.

After upload, verify installation with `python -m pip install hqg-datafeed`
in a fresh environment and check the
[PyPI project page](https://pypi.org/project/hqg-datafeed/).

Reference: [Python Packaging User Guide](https://packaging.python.org/en/latest/tutorials/packaging-projects/).
