# Installing the Impira SDK

## Requirements

* [Python >= 3.8](https://www.python.org/downloads/)
* Mac OS X or Linux

* [soffice](https://www.systutorials.com/docs/linux/man/1-soffice/) (LibreOffice's command line tools) if you are using `impira generate`

:::{note}
This SDK is actively tested with the above requirements. We have users using Windows as well although it is not tested regularly.
Please reach out if you run into any issues on Windows or another platform.
:::

## Installing with pip

To install the Impira SDK using pip, simply run:

```bash
$ pip install impira
```

This will install both the Python libraries and Impira command line tool (`impira`) in your Python environment. We highly recommend
installing Impira in a virtual environment.

:::{note}
The SDK depends on `boto3` to work with AWS Textract which is used by the [`infer-fields`](commands/infer-fields) command. Neither `boto3` nor
Textract is required to operate the Impira API. In the future, this dependency may be installed via an optional extension.
:::

(authenticating)=
## Setting up the Impira API

To use Impira, you need to know your organization's name and obtain an API token. You can find your organization's
name in the URL you visit to access Impira. For example, when you login, if the URL is 
`https://app.impira.com/o/acme-corp-1a23/collections`, then your organization's name is `acme-corp-1a23`.
For instructions on obtaining an API token, please visit the 
[Impira docs](https://www.impira.com/documentation/impira-read-api#toc-creating-an-api-token).

Many commands throughout the Impira CLI reference a `--collection` parameter. This collection id can be found by navigating to a 
collection in the application, and copying the identifier after `fc`. For example, for a collection at a 
URL like `https://app.impira.com/o/acme-corp-1a23/fc/07b71143a26b7163`, the collection's id is `07b71143a26b7163`.

## Developing

To develop the Impira SDK, you can clone the repository and run `make develop` to create a local virtual environment. Within this environment, changes to the source code will automatically update the `impira` command.

```bash
$ git clone git@github.com:impira/impira-python.git
$ cd impira-python
$ make develop
```
