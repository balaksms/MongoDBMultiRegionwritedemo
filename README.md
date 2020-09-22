---
page_type: sample
languages:
- java
products:
- azure
description: "Azure Cosmos DB is a fully managed globally distributed, multi-model database service, transparently replicating your data across any number of Azure regions."
urlFragment: azure-cosmos-db-mongodb-java-geo-readpreference
---

# Using ReadPreference command with Azure Cosmos DB for MongoDB API
Azure Cosmos DB is a fully managed globally distributed, multi-model database service, transparently replicating your data across any number of Azure regions. You can elastically scale throughput and storage, and take advantage of fast, single-digit-millisecond data access using the API of your choice backed by 99.999 SLA. This sample shows you how to use ReadPreference command against Azure Cosmos DB for MongoDB API from a Java application.

## Running this sample

* Before you can run this sample, you must have the following prerequisites:

   * An active Azure account. If you don't have one, you can sign up for a [free account](https://azure.microsoft.com/free/). Alternatively, you can use the [Azure Cosmos DB Emulator](https://docs.microsoft.com/azure/cosmos-db/local-emulator) for this tutorial.
   * Eclipse Oxygen 

* Then, clone this repository.

* Next, substitute the `connectionString`, `readTargetRegion` in *src\main\resources\config* with your Cosmos DB account's values. 

* Run your application.

## About the code
The code included in this sample is intended to illustrate using ReadPreference with a Java application that connects to Azure Cosmos DB for MongoDB API.

## More information

- [Azure Cosmos DB](https://docs.microsoft.com/azure/cosmos-db/introduction)
- [Azure Cosmos DB for MongoDB API](https://docs.microsoft.com/azure/cosmos-db/mongodb-introduction)
- [MongoDB Java driver](http://mongodb.github.io/mongo-java-driver/)
