# Hive MapR Database JSON Connector Tutorial
This repository will contain examples to discover the key features of the Hive connector for MapR Database JSON table. 

## Contents
* [Prerequisites](#prerequisites)
* [Configuring and check the environment for correct work](#)
* [Creating Table](#)
	- [Creating a MapR DB JSON Table and Hive Table Using Hive](#)
	- [Creating a Hive table that exists on MapR Database JSON Table](#)
* [Load data stored in JSON format to MapR DB using the Hive connector](#)
	- [Load data into MapR-DB with Hive connect  from a local file system](#)
	- [Load data into MapR-DB with Hive connect  from MaprFs](#)
* [Querying Statements](#)
	- [Complex Fields](#)
	- [Nested Structures](#)
* [Inserting Statements](#)
	- [Single-row insert](#)
	- [Multiple-row insert](#)
	- [Overwriting data](#)
* [Updating Statements](#)
 	- [Primitive Data Types](#)
	- [Complex Data Types](#)
	- [Complex Nested Data Types](#)
* [Merging Statements](#)
* [Limitations](#Limitations)

## Prerequisites
* MapR Converged Data Platform 6.1 with Apache Hive and Apache Drill or [MapR Container for Developers](#https://mapr.com/docs/home/MapRContainerDevelopers/MapRContainerDevelopersOverview.html).
* JDK 8

### Configuring and check the environment for correct work.
To create a table, first run hive and create databese test:

```sh
$ hive
hive> CREATE DATABASE demo;
```
/// TODO INSERT INTO <table> 

### Creating Table
The Hive connector supports the creation of MapR Database based Hive tables. You can create a JSON table on MapR Database and load CSV data and/or JSON files to MapR Database using the connector. 

##### Creating a MapR DB JSON Table and Hive Table Using Hive
/// TODO

##### Creating a Hive table that exists on MapR Database JSON Table
/// TODO

#### Load data stored in JSON format to MapR DB using th Hive connector
/// TODO

##### Load data into MapR-DB with Hive connectofrom from local file system
/// TODO

##### Load data into MapR-DB with Hive connectofrom from MaprFs
/// TODO

#### Querying Statements
/// TODO

##### Complex Fields
/// TODO

##### Nested Structures
/// TODO

#### Inserting Statements
/// TODO

##### Single-row insert
/// TODO

##### Multiple-row insert
/// TODO

##### Overwriting data
/// TODO

#### Updating Statements
/// TODO

##### Primitive Data Types
/// TODO

##### Complex Data Types
/// TODO

##### Complex Nested Data Types
/// TODO

#### Merging Statements
/// TODO

### Limitations
/// TODO