# Hive MapR Database JSON Connector Tutorial
The repository contains examples explain the key features of using Hive connector for MapR Database JSON table. 

## Contents 

1. [CLI examples for using Hive MapR Database JSON Connector](doc/tutorials/001-hive-connector-cli.md)
2. [Java examples for using Hive MapR Database JSON Connector](doc/tutorials/002-hive-connector-java.md)
3. [The official Hive connector for MapR Database JSON table documentation](https://mapr.com/docs/61/Hive/ConnectingToMapR-DB.html)
4. [Apache Hive with MapR Data Platform integration](https://mapr.com/products/apache-hive/)
5. [Free course on MapR Academy "Query and Store Data with Apache Hive in MapR Data Platform"](https://learn.mapr.com/da-440-apache-hive-essentials)


### Prerequisites
* MapR Converged Data Platform 6.1 with Apache Hive and Apache Drill or [MapR Container for Developers](https://mapr.com/docs/home/MapRContainerDevelopers/MapRContainerDevelopersOverview.html).
* JDK 8
* Maven 3.5.2
* All datasets needed are located `/dataset`


### JavaDoc
The project includes JavaDoc which is the de facto industry standard for documenting Java classes. 
To open the project's site run from the project directory (where pom.xml is located):

```
$ mvn site
``` 

Then open your favorite browser and navigate to:

```
http://localhost:63342/mapr-db_hive_connector-sample/target/site/apidocs/index.html
```

