# Impira Python SDK and CLI

Impira enables you to get everything you need from your PDFs, scanned documents, images, and more — with the help of machine learning. This API allows you to access Impira programatically through Python and the command line.

**NOTE: This SDK is currently under active development and is likely to break backwards compatibility between point releases. We will update this disclaimer when this changes.**

## Full documentation

Below is an abbreviated set of documentation to help you get started. You can visit the full documentation by visiting the [Impira SDK docs](https://impira.github.io/impira-python/index.html).

## Requirements

This SDK is tested with Python 3.8+ on Mac OS X and Linux systems. We have users using Windows as well; however, this scenario is not tested automatically. Please reach out if you run into any issues on Windows or another platform.

## Installation

You can install the Impira Python SDK directly through pip:

```bash
$ pip install impira
```

### Development mode

If you would like to install the SDK to develop locally, you can run the following:

```bash
$ git clone git@github.com:impira/impira-python.git
$ cd impira-python
$ make develop
```

This will create a virtualenv locally and install the library to it in a manner that automatically updates as you change the source code.

## CLI overview

See [CLI Commands](https://impira.github.io/impira-python/commands.html) in the docs.

## SDK overview

The Impira Python SDK includes utilities to upload and label files, insert and update data, and query data. The core abstraction is the `Impira` object which represents an authenticated connection to your organization. The SDK makes heavy use of the [pydantic](https://pydantic-docs.helpmanual.io/) library to automatically build up and validate function arugments.

### Authenticating

To connect to an org, you simply instantiate the `Impira` object with your organization's name and API token. You can find your organization's name in the URL you visit to access Impira. For example, when you login, if the URL is `https://app.impira.com/o/acme-corp-1a23/collections`, then your organization's name is `acme-corp-1a23`. For instructions on obtaining an API token, please visit the [Impira docs](https://www.impira.com/documentation/impira-read-api#toc-creating-an-api-token). For security reasons, we highly recommend storing both the organization name and API key in configuration or environment variables, not directly in the code itself. For the purpose of these examples, we will use the environment variables `IMPIRA_ORG_NAME` and `IMPIRA_API_KEY`.

```python
from impira import Impira
import os

impira_api = Impira(os.environ["IMPIRA_ORG_NAME"], os.environ["IMPIRA_API_KEY"])
```

When you instantiate the Impira API, it will automatically issue a `ping` request to validate your credentials and raise an `InvalidRequest` exception if it fails. You can disable this behavior by passing `ping=False` to the constructor.

### Referencing a collection

Many function calls in the API reference a `collection_id` parameter. This id can be found by navigating to a collection in the application, and copying the identifier after `fc`. For example, for a collection at a URL like `https://app.impira.com/o/acme-corp-1a23/fc/07b71143a26b7163`, the `collection_id` is `07b71143a26b7163`.

### Uploading

To upload one or more files, you must provide at a minimum a name and path (either local or a URL) for each file. The specification is defined in the `FilePath` type. You can also optionally specify a `collection_id` or `None` to upload the file globally (to "All files").

```python
# Upload a file on your local machine
uids = impira_api.upload_files(collection_id, [
    {"name": "foo.pdf", "path": "/Users/me/Desktop/foo.pdf"}
])
                                             
# Upload multiple files by specifying their URLs
uids = impira_api.upload_files(collection_id, [
    {"name": "foo.pdf", "path": "http://website.com/foo.pdf"},
    {"name": "bar.pdf", "path": "http://website.com/bar.pdf"},
])
```

The `uids` variable is a list with a `uid` for each file. A file's `uid` is its unique identifier throughout the system. A file that belongs to more than one collection will have the same `uid` in each. You can also optionally specify your own `uid` while uploading a file. If two files have the same `uid`, the system will automatically replace the former with the latter, effectively versioning the file. For more information on uploading files, visit the [Upload API docs](https://www.impira.com/documentation/impira-write-api).

### Polling for results

Impira's API is asynchronous, meaning that uploading files and retrieving predictions from them occur in two separate API requests. While there are many [advanced ways to query for data using IQL](https://www.impira.com/documentation/iql-for-advanced-queries), the SDK offers a simple `poll_for_results()` method that allows you to wait for results to be available for your uploads. Using the `uids` returned from `upload_files()` (as demonstrated above), you can simply run something like

```python

for row in impira_api.poll_for_results(collection_id, uids):
    print(row)
```

to retrieve each prediction. Note that `poll_for_results()` returns a generator, so you must iterate through its output to retrieve each result.

### Running IQL queries

You can run arbitrary IQL queries through the API by simply invoking the `query()` method. The response is exactly the same format as [the read API](https://www.impira.com/documentation/impira-read-api) and the SDK also supports poll mode (by passing the `mode="poll"` argument). For example,

```python

response = impira_api.query(
    "@`file_collections::%s`[uid] highest:Uploaded limit:10" % (collection_id)
)
last_10_uids = [row["uid"] for row in response["data"]]
```

will retrieve the `uid` of each of the last 10 files uploaded to the collection. For more information on how to construct IQL queries, see the [IQL docs](https://www.impira.com/documentation/iql-for-advanced-queries).

## Examples

The examples directory contains end-to-end working examples for how to use the SDK. 
* `upload_files.py` walks through uploading a file, either locally or through a URL, and then waiting for its results.
* `download_all_collections_query.py` creates an IQL query that queries all data across all collections

## License

[MIT License](LICENSE). Copyright 2021 Impira Inc.
