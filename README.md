# Tecton Examples Repository

This repository provides concrete code examples demonstrating how to use Tecton for feature engineering in machine learning applications. You'll find a comprehensive collection of examples implemented in two ways:
- Using Spark for Batch and Streaming features
- Using Rift, Tecton's Python-native compute engine, for Batch, Streaming and Real-Time features

Please navigate to the [spark](./spark/) or [rift](./rift/) folder depending on which compute you use. If you already know, use, and love Spark or Databricks, you can start with the [spark](./spark/) folder. If you prefer using Python and/or SQL without the need for a Spark cluster, check out the [rift](./rift/) folder.

The examples cover a wide range of feature engineering patterns and techniques, from simple aggregations to complex feature pipelines, all in the context of real-world use cases like fraud detection, recommendation systems and ads serving.

#### Usage
* Install tecton using `pip install tecton>=1.1`
* Log into your tecton cluster using `tecton login <cluster_name>`
* Create a new workspace using `tecton workspace create <workspace_name>`
* `cd` into the `spark` or `rift` folder depending on which compute you use
* Initialize repo using `tecton init`
* Apply the features defined here using `tecton apply`

#### Docs
* [Tecton's documentation](https://docs.tecton.ai/) has information about how to use Tecton to create, manage, and serve operational ML features
* [CLI installation instructions](https://docs.tecton.ai/docs/setting-up-tecton/development-setup/installing-the-tecton-cli)
