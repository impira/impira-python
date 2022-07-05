# Snapshotting an Impira collection

The Impira CLI allows you to save the state of an Impira collection, including the files, fields (schema), and user-provided labels.
You can use this information to backup a collection and reconstruct it elsewhere. There are flags that allow you to do this
completely from scratch (uploading files, creating fields, and adding labels) or just pieces of this on collections that are
partially setup.

To use the `snapshot` command, you must know the credentials for your Impira account. See {ref}`authenticating`
for instructions on how to obtain them.

## Running the snapshot command

To snapshot a collection, run a command like

```bash
$ impira snapshot \
    --org-name YOUR_ORG_NAME \
    --api-token YOUR_API_TOKEN \
    --collection YOUR_COLLECTION_ID
```

By default, the command will save a file named `manifest.json` containing all of the relevant metadata in a directory inside of
your system's temporary directory. You can use this directory in the [`bootstrap`](bootstrap) command to setup a new
collection based on the data you snapshotted.

## Advanced options

* By default `snapshot` will save its data in your system's temporary directory. If you would like to save the data somewhere else,
you can specify a directory via the `--data` option.
* By default, `snapshot` will save the files' URLs, not the files themselves for performance reasons. You can specify the
`--download-files` flag to save files locally.
* To avoid naming conflicts, `snapshot` will append a unique identifier to the end of each file name. In certain cases, however,
you may want to bootstrap a collection usings its original file names (e.g. to avoid re-uploading them). To preserve the
original filenames, you can pass the `--original-names` flag. This command expects each file to have a unique name
(and will error otherwise).

For a full list of options, run `impira snapshot --help`.
