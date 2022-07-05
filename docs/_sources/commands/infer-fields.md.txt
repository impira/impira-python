# Inferring fields (using AWS Textract)

The Impira CLI allows you to use AWS Textract to detect key:value pairs in a document. This can be useful to avoid
having to create a schema, field-by-field, in Impira. Currently, `impira infer-fields` is capable of inferring
text and selection (checkbox) fields. Although Textract does not distinguish between text, number, and timestamp
types, [`impira bootstrap`](bootstrap) can automatically determine the correct field types within Impira.

## Requirements

The `infer-fields` command requires that your AWS credentials are setup for `boto3` to run. The
[AWS documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
describes several ways to do so. The command does not allow you to specify your credentials explicitly.

The credentials must have access to Textract and to read/write files from Amazon S3. It will try to
create an `impira-cli` bucket in your account automatically if one does not already exist, so it must have
permissions to do that, unless you specify one explicitly on the command line.

## Inferring fields

You can run a command like the following:

```bash
$ impira infer-fields /path/to/document.ext 
```

By default, the `infer-fields` command will save the captured fields and data in your system's temporary directory and
output its full path at the end of execution. You can specify your own data directory optionally with the `--data` argument.
You can use the directory that `infer-fields` outputs in the [`bootstrap`](bootstrap) command to automatically setup 
an Impira collection with the inferred fields and training data. Although you can bootstrap an Impira collection with as
little as one file, you can run `impira infer-fields` across multiple files of the same document type to generate additional
training data.

For a full list of options, run `impira infer-fields --help`.
