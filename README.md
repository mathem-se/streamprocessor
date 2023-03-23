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


### Install Java and Maven

Update brew
```bash
brew update && brew upgrade
```

Install java 11 and maven
```bash
brew install java11 maven
```

### Compile project
```bash
mvn clean compile
```

### Format code
```bash
mvn spotless:apply
```

### Run tests
1. To test all modules in the package, run:
	```bash
	mvn test
	```
1. To test an individual module:
	```bash
	mvn test -pl <MODULE>
	```
	for example:
	```bash
	mvn test -pl core
	```
1. To test an individual class inside a module, run:
	```bash
	mvn test -pl <MODULE> -Dit.test=<CLASS>
	```
	for example:
	```bash
	mvn test -pl core -Dit.test=SerializeMessageToRowFnTest
	```

### Run a pipeline locally

Make sure you are logged in to gcloud and then assume a GCP service account to be able to access the datacatalog api.

1. Start by compiling and installing the root project. It will fail on pipelines but succeed on the core components which is what's needed to run the pipeline.
	``` bash
	mvn clean install -U
	```
1. Then run a pipeline.
	```bash
	cd streamprocessor/pipelines/<PIPELINE>
	mvn -Pdataflow-runner -e compile exec:java \
	-Dexec.mainClass=org.streamprocessor.pipelines.<PIPELINE> \
	-Dexec.args="--bigQueryDataset=<BIGQUERY_DATASET> \
	--project=<GCP_PROJECT> \
	--schemaCheckRatio=<SCHEMA_CHECK_RATIO> \
	--firestoreProjectId=<FIRESTORE_GCP_PROJECT> \
	--deadLetterTopic=projects/<GCP_PROJECT>/topics/<DEADLETTER_TOPIC> \
	# Regular pipelines
	--backupTopic=projects/<GCP_PROJECT>/topics/<BACKUP_TOPIC> \
	--inputSubscription=projects/<GCP_PROJECT>/subscriptions/<SUBSCRIPTION_NAME> \
	# Backup pipeline
	--pipelineType=<PIPELINE_TYPE> \
	--backfillQuery=\"<BACKFILL_QUERY>\""

	```
where
- `<PIPELINE>` is the name of the main class for the pipeline you want to run e.g Dynamodb
- `<BIGQUERY_DATASET>` is the bigquery dataset to write the data to
- `<GCP_PROJECT>` is the name of the GCP project
- `<SCHEMA_CHECK_RATIO>` is the ratio of how often to check for schema updates e.g 0.01 would check once every 100 calls
- `<FIRESTORE_GCP_PROJECT>` is the name of the firestore gcp project that keeps run resources
- `<DEADLETTER_TOPIC>` is the name of the deadletter topic
- *regular pipe* `<BACKUP_TOPIC>` is the name of the backup topic
- *regular pipe* `<SUBSCRIPTION_NAME>` is the name of the pub/sub subscription to pull data from
- *backfill pipe* `<PIPELINE_TYPE>` is actually the transform pipeline used on the data you send in, which also corresponds to the source provider and the dataset that the data ends up in (`dynamodb|salesforce`).
- *backfill pipe* `<BACKFILL_QUERY>` is the query used against backup table which extracts topic `data, attributes, publish_time` from some period.

If you update the something in the core modules you need to recompile the root project and restart the pipeline to see the changes.


### Build and push to GCP

To build your project and push it to GCP artifact registry and cloud storage, run the following:
```bash
bash build_and_push.sh <GCP-PROJECT-ID> <MODULE> <VERSION>
```
where
- `<GCP-PROJECT-ID>` is the GCP project id where you want to publish to.
- `<MODULE>` is one of the pipelines in [./pipelines](https://github.com/mhlabs/streamprocessor/tree/DATA-2781-public-streamprocessor), e.g. `dynamodb`
- `<VERSION>` is the version you want to publish as.

This will end up with the following artifacts in your GCP project:

`europe-west1-docker.pkg.dev/<GCP-PROJECT-ID>/streamprocessor/<MODULE>:<VERSION>`
and
`gs://<GCP-PROJECT-ID>-streamprocessor/images/<MODULE>-image-spec-<VERSION>.json`



note that the `build_and_push.sh` script assumes that you have:
- a project called `<GCP-PROJECT-ID>`
- enabled cloud storage and artifact registry
- created a repository in artifact registry called `streamprocessor`
- created a bucket in cloud storage called `<GCP-PROJECT-ID>-streamprocessor`

## What's included
### Pipelines
**TODO**

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
