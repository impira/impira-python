# Bootstraping an Impira collection

The Impira CLI allows you to setup an Impira collection based on results from another service (using the
[`infer-fields`](infer-fields)) or Impira (using the [`snapshot`](snapshot) command). Each of these commands
outputs a directory containing a file named `manifest.json` which contains information about files, fields, and
labels to reconstruct the collection.

To use the `bootstrap` command, you must know the credentials for your Impira account. See {ref}`authenticating`
for instructions on how to obtain them.

## Running the bootstrap command

To bootstrap a collection, run a command like

```bash
$ impira bootstrap -d /path/to/snapshotted/data/ \
    --org-name  YOUR_ORG_NAME \
    --api-token YOUR_API_TOKEN \
```

The command will log its progress as it runs and print a link to the new collection with an automatically generated
name. You can rename the collection whatever you'd like in the Impira app.

### Reusing existing state

By default, the `bootstrap` command will create a new collection and then upload the files, setup the fields, and 
label the files defined in the snapshotted data directory automatically. You can optionally specify a `--collection` 
flag to work inside of an existing collection and a `--skip-upload` flag to reuse the files in the collection instead 
of uploading them from scratch. 

`bootstrap` will automatically reuse existing fields if they exist and create new ones that do not. If there are type
conflicts, it will print a warning message and may fail to provide labels.

## Common Patterns

### From AWS Textract

Impira requires you to setup the fields you wish to extract and provide at least one label. Although this method allows
Impira to provide a very high level of accuracy, it can be cumbersome to setup on files with lots of fields, like ACORD forms.

The [`infer-fields`](infer-fields) command allows you to automatically guess the fields and labels in a file using AWS Textract.
You can then use this to bootstrap a new collection in Impira, and then simply correct any inaccuracies within the Impira UI.

```bash
# This will output a directory like data/capture/document.ext-98bf
$ impira infer-fields -d data \
    /path/to/document.ext 

# This will output a collection and its id
$ impira bootstrap -d data/capture/document.ext-98bf \
    --org-name YOUR_ORG_NAME \
    --api-token YOUR_API_TOKEN \
```

If you run `infer-fields` on multiple files of the same document type, you can add more files to the collection too:

```bash
# Upload an additional document
$ impira bootstrap -d data/capture/document-2.ext-a81e \
    --org-name YOUR_ORG_NAME \
    --api-token YOUR_API_TOKEN \
    --collection COLLECTION_ID
```

:::{warning}
Textract does not necessarily produce the same field names between documents of the same type. For example, two ACORD 25 forms might produce fields with slightly different names like `INSURER A` and `INSURER A:`. The Impira CLI will do its best to reconcile fields with different names, but you may end up with duplicate fields in Impira if you bootstrap from multiple files.
:::

### From another Impira collection

The [`snapshot`](snapshot) command can be used together with the `bootstrap` command to save and restore Impira collections.
This is useful for quality assurance and to experiment with new labeling approaches without disrupting an existing collection.

```bash
# This will output a directory like data/snapshot/07b71143a26b7163-98bf
$ impira snapshot -d data \
    --org-name YOUR_ORG_NAME \
    --api-token YOUR_API_TOKEN \
    --collection YOUR_COLLECTION_ID

# This will output a collection and its id
$ impira bootstrap -d data/snapshot/07b71143a26b7163-98bf \
    --org-name YOUR_ORG_NAME \
    --api-token YOUR_API_TOKEN \
```

## Advanced options

* To use an existing collection rather than creating a new one, pass in the collection's id using the `--collection` flag.
* By default, `bootstrap` will upload files into the collection. However, if you pre-populate the collection with each of the
relevant files (by name), then you can use `--skip-upload` to use the existing files.
* The `bootstrap` command attempts to pick specific types (like `number` and `timestamp`) using Impira's named entity recognition. You can disable this behavior with `--skip-type-inference`.

For a full list of options, run `impira bootstrap --help`.
