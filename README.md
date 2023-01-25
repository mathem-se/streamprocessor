<p align="center">
    <img src="./images/streamprocessor.logo.png" alt="StreamProcessor logo" width="200">
</p>

<p align="center">(replace logo)</p>
<h3 align="center">StreamProcessor</h3>

<p align="center">
  Streaming Beam pipeline to serialize, tokenize, and load data to BigQuery and Pubsub.
  <br>
  <a href="https://github.com/mhlabs/streamprocessor/docs/main.md">Docs</a>
  ·
  <a href="https://blog.mhlabs.com/">Blog</a>
  ·
  <a href="https://github.com/mhlabs/streamprocessor/issues/new?assignees=-&labels=bug&template=bug_report.yml">Report bug</a>
  ·
  <a href="https://github.com/mhlabs/streamprocessor/issues/new?assignees=&labels=feature&template=feature_request.yml">Request feature</a>
</p>

## Table of contents

- [Quick start](#quick-start)
- [What's included](#whats-included)
- [FAQs](#faq)

## Quick start

This project is set up to enable you to publish an image of a Dataflow pipeline (Apache Beam@Java) to a GCP Artifact Registry. It also instructs you how to create such a container and how to test the pipeline (from topic message generation to running the StreamProcessor pipeline).

**Make sure** you have java@11 installed. 

```bash
brew install java11
```

## What's included
### Pipelines

#### json-tokenize

Tokenizes json messages from pubsub and writes to BigQuery and Pubsub topics.

## FAQ
### Is StreamProcessor open source?

Yes, it is licensed under Apache 2.0, read the LICENSE file

### Who maintains StreamProcessor?

StreamProcessor is founded by Robert Sahlin and under active development by the data engineering team at Mathem.

### Who sponsors StreamProcessor?

Mathem, the leading online grocery business in Sweden, sponsors the development of StreamProcessor and is a heavy user of it.

## Contributing

Please read through our [contributing guidelines](https://github.com/mhlabs/streamprocessor/CONTRIBUTING.md). Included are directions for opening issues, coding standards, and notes on development.

<!-- Moreover, if your pull request contains Java patches or features, you must include [relevant unit tests](https://github.com/..). Adhere to [Code Guide](https://github.com/some/code-guide)

Editor preferences are available in the [editor config](https://github.com/mhlabs/streamprocessor/.editorconfig) for easy use in common text editors. Read more and download plugins at <https://editorconfig.org/>. -->