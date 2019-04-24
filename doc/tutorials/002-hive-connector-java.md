# Java examples for using Hive MapR Database JSON Connector
The examples to discover the key features of the Hive connector for MapR Database JSON table using Java. 

## Contents
* [Installing and configuring MapR Client](#installing-and-configuring-mapr-client)
* [Clone and build the project](#clone-and-build-the-project)
* [Establishing a connection with Hive](#establishing-a-connection-with-hive)
* [Creating tables](#creating-tables)
* [Showing all tables in the database](#showing-all-tables-in-the-database)
* [Loading data to MapR DB using the Hive connector](#loading-data-to-mapr-db-using-the-hive-connector)
* [Querying statements](#querying-statements)
  - [Primitive data types](#primitive-data-types)
  - [Complex data type](#complex-data-type)
  - [Complex nested structures](#complex-nested-structures)
* [Inserting statements](#inserting-statements)
  - [Single-row insert](#single-row-insert)
  - [Multiple-row insert](#multiple-row-insert)
  - [Overwriting data](#overwriting-data)
* [Updating Statements](#updating-statements)
  - [Update primitive data types](#update-primitive-data-types)
  - [Update complex data types](#update-complex-data-types)
  - [Update complex nested data types](#update-complex-nested-data-types)
  - [Update statement limitations](#update-statement-limitations)
* [Merging statements](#merging-statements)
  - [Merge statement limitations](#merge-statement-limitations)
* [Run the application from the IDE](#run-the-application-from-the-ide)




### Installing and configuring MapR Client

> Note that if you are using MapR Container for Developers, the MapR Client is automatically installed and configure you do not need to do it again.

The `mapr-client` package must be installed on the host / node where you will be building and running your applications. 
This package installs all of the MapR Libraries needed for application development regardless of programming language or 
type of MapR-DB table (binary or JSON).

Follow the instructions of MapR Documentation:

* [Installing MapR Client](https://maprdocs.mapr.com/home/AdvancedInstallation/SettingUptheClient-install-mapr-client.html)

> In case if you going to clone and build the project on the node when Hive is installed you don't need configuring MapR Client. 

### Clone and build the project
1. Clone or download the project from the repository. Run the maven command:

```
$ git clone url
```
2. Navigate to the root project directory (where pom.xml file is located) and run:

```
$ mvn clean package
```

> If you did not install MapR Client copy the application jar to your MapR cluster, for example:

```
scp ./target/mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar mapr@mapr60:/home/mapr/
```

> Where `mapr60` is one of the node of the MapR cluster.


### Establishing a connection with Hive

You can configure connection properties in a few way:

##### 1. By pass the configuration parameters via command line arguments.

>In this case the priority is the highest.

The arguments order:

```
driverClassName = args[0];
url = args[1];
host = args[2];
port = args[3];
database = args[4];
user = args[5];
password = args[6];
```

For example:

```
java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.utils.ShowTables org.apache.hive.jdbc.HiveDriver jdbc:hive2:// localhost 10000 default mapr mapr
```

##### 2. By set connection parameters in to system variables. In this case the app examples will use them.
>In this case, the app will use them if the command line arguments not set, or they are bad.

```
export HIVE_DRIVER_CLASS_NAME=org.apache.hive.jdbc.HiveDriver
export HIVE_DRIVER_CONNECTION_URL=jdbc:hive2://
export HIVE_HOST=localhost
export HIVE_PORT=10000
export HIVE_DATABASE_NAME=default
export HIVE_DATABASE_USER=mapr
export HIVE_DATABASE_USER_PASSWORD=mapr
```

##### 3. By set configuration connection parameters into property file, located `src/main/resources/hive_mapr_db.properties`:
>In this case the properties will use as default. The priority is the lowest.

```
uri=jdbc:hive2://192.168.33.13:10000/
user=mapr
password=mapr
databaseName=default
driverClassName=org.apache.hive.jdbc.HiveDriver
``` 

### Creating tables
This section describes how to create tables in MapR DB JSON tables, using the Hive connector from Java.  


The class [`CreateTablePrimitiveTypes`](src/main/java/com/mapr/hiveconnector/examples/creating/CreateTablePrimitiveTypes.java) demonstrates this.  

In a terminal window type the following commands to run the sample:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.creating.CreateTablePrimitiveTypes
```

This program creates the `primitive_types_from_java` table in the database.

Verifying the table is created in Hive, demonstrate utility class [`ShowTables`](src/main/java/com/mapr/hiveconnector/utils/ShowTables.java):
In a terminal window type the following command to find out the table `primitive_types_from_java` is created.

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.utils.ShowTables  
```

Inserting data into table `primitive_types_from_java` from Java demonstrate the class [`InsertIntoTablePrimitiveTypes`](src/main/java/com/mapr/hiveconnector/examples/inserting/InsertIntoTablePrimitiveTypes.java).
In a terminal window type the following command:.

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.inserting.InsertIntoTablePrimitiveTypes  
```

Verifying that the data are inserted using Hive, demonstrate the class [`SelectFromTablePrimitiveTypes`](src/main/java/com/mapr/hiveconnector/examples/utils/ShowTables.java):
In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.querying.SelectFromTablePrimitiveTypes 
```

>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#creating-tables)


### Showing all tables in the database
This section describes how we can get the list of all tables in current database `default` from Java.


The class [`ShowTables`](src/main/java/com/mapr/hiveconnector/utils/ShowTables.java) demonstrates this.  

In a terminal window, connected as mapr user type the following commands to run the sample:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.utils.ShowTables 
```

>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#showing-all-tables-in-the-database)


### Loading data to MapR DB using the Hive connector

This section describes how to load data in MapR DB JSON tables, using the Hive connector using Java.


We will load data stored located in `/dataset/artists.json` file. 
Copy the json file to the node's local file system or Mapr Fs:
 
```
$ scp ./dataset/artists.json mapr@mapr60:/tmp/datasets/
```

**artists**
Contains `10281` Artist JSON documents, which are ready to be imported into MapR-DB JSON Table. 

<details> 
  <summary>Example of such Artist document</summary>
  
  ```
  {
     "name": "David Cook",
     "gender": "Male",
     "area": "United States",
     "deleted": false,
     "mbid": "966e1095-b172-415c-bae5-53f8041fd050",
     "_id": "966e1095-b172-415c-bae5-53f8041fd050",
     "slug_name": "david-cook",
     "slug_postfix": {
        "$numberLong": 0
     },
     "MBID": "966e1095-b172-415c-bae5-53f8041fd050",
     "disambiguation_comment": "American Idol",
     "albums": [
        {
           "cover_image_url": "http://coverartarchive.org/release/78d08954-e79f-4a80-929d-71cc0ecc7b9d/6964754870.jpg",
           "slug": "analog-heart-0",
           "name": "Analog Heart",
           "id": "78d08954-e79f-4a80-929d-71cc0ecc7b9d"
        },
        {
           "cover_image_url": "http://coverartarchive.org/release/1fdff2a1-1bdf-499a-a50c-e5d742958094/10875910782.jpg",
           "slug": "david-cook-1",
           "name": "David Cook",
           "id": "1fdff2a1-1bdf-499a-a50c-e5d742958094"
        },
        {
           "cover_image_url": "http://coverartarchive.org/release/d4cccd1c-61fb-4939-aa53-49798314724e/2144368240.jpg",
           "slug": "david-cook-2",
           "name": "David Cook",{"country":"Switzerland","languages":["German","French","Italian"],"religions":{"catholic":[10,20],"protestant":[40,50]}}
        },
        {
           "cover_image_url": "http://coverartarchive.org/release/40184dbe-40fa-4845-b7e6-ca20242853eb/7976913345.jpg",
           "slug": "the-last-goodbye-0",
           "name": "The Last Goodbye",
           "id": "40184dbe-40fa-4845-b7e6-ca20242853eb"
        },
        {
           "cover_image_url": "http://coverartarchive.org/release/e21facb9-ecf7-407e-990a-ff465ace43a1/9322135862.jpg",
           "slug": "this-loud-morning-2",
           "name": "This Loud Morning",
           "id": "e21facb9-ecf7-407e-990a-ff465ace43a1"
        },
        {
           "cover_image_url": "http://coverartarchive.org/release/528568d0-ce68-42b8-b122-f57a57763637/2466862952.jpg",
           "slug": "this-loud-morning-3",
           "name": "This Loud Morning",
           "id": "528568d0-ce68-42b8-b122-f57a57763637"
        },
        {
           "cover_image_url": "http://coverartarchive.org/release/4bb7977f-c67a-4bf6-ab26-994d59a06717/12602733617.jpg",
           "slug": "this-quiet-night-0",
           "name": "This Quiet Night",
           "id": "4bb7977f-c67a-4bf6-ab26-994d59a06717"
        },
        {
           "cover_image_url": "http://coverartarchive.org/release/924fab61-e21c-4065-a711-f2f55fe2e6d9/1452573615.jpg",
           "slug": "always-be-my-baby-0",
           "name": "Always Be My Baby",
           "id": "924fab61-e21c-4065-a711-f2f55fe2e6d9"
        },
        {
           "cover_image_url": "http://coverartarchive.org/release/0014a89f-978c-401f-b3cb-86d14d41ea0d/12905818584.jpg",
           "slug": "digital-vein-0",
           "name": "Digital Vein",
           "id": "0014a89f-978c-401f-b3cb-86d14d41ea0d"
        },
        {
           "cover_image_url": "http://coverartarchive.org/release/1cd1b0f8-a049-484f-a2a2-73bf7bbb8295/17079514876.jpg",
           "slug": "gimme-heartbreak-0",
           "name": "Gimme Heartbreak",
           "id": "1cd1b0f8-a049-484f-a2a2-73bf7bbb8295"
        }
     ],
     "profile_image_url": "https://upload.wikimedia.org/wikipedia/commons/a/a0/David_Cook_Toads_cropped.jpg",
     "images_urls": [],
     "begin_date": {
        "$dateDay": "1982-12-20"
     },
     "rating": 2.919355
  }
  ```
  
</details>

The class [`LoadDataToTables`](src/main/java/com/mapr/hiveconnector/examples/loading/LoadDataToTables.java) demonstrates 
how to load data in MapR DB JSON tables, using the Hive connector. 

In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.loading.LoadDataToTables  
```

>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#loading-data-to-mapr-db-using-the-hive-connector)


#### Querying statements

This section describes how to use the `SELECT` statement within MapR DB JSON tables, using the Hive connector from Java. 


##### Primitive data types

The class [`SelectFromTablePrimitiveTypes`](src/main/java/com/mapr/hiveconnector/examples/querying/SelectFromTablePrimitiveTypes.java) demonstrates how we can
select primitive data types from table `primitive_types_from_java`:   

In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.querying.SelectFromTablePrimitiveTypes
```

>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#primitive-data-types)


##### Complex data type

The class [`QueryingComplexDataType`](src/main/java/com/mapr/hiveconnector/examples/querying/QueryingComplexDataType.java) demonstrates how we can
select complex data types from table `artists_java`:   

In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.querying.QueryingComplexDataType
```

>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#complex-data-type)


##### Complex nested structures


The class [`QueryingComplexNestedStructures`](src/main/java/com/mapr/hiveconnector/examples/querying/QueryingComplexNestedStructures.java) demonstrates how we can
select complex nested structures from table `json_nested_java`:   

In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.querying.QueryingComplexNestedStructures 
```

<details> 
  <summary>Complex nested structures source</summary>
  
```
{"country":"Switzerland","languages":["German","French","Italian"],"religions":{"catholic":[10,20],"protestant":[40,50]}}
```
</details>


>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#complex-nested-structures)


#### Inserting statements
This section describes how to use the `INSERT INTO` statement to insert or overwrite rows in nested MapR DB JSON tables, using the Hive connector from Java.


##### Single-row insert
The `INSERT INTO` statement to insert a single table row into a nested MapR DB table. 
To demonstrate this function use class [`SingleRowInsert`](src/main/java/com/mapr/hiveconnector/examples/inserting/SingleRowInsert.java)

In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.inserting.SingleRowInsert  
```

>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#single-row-insert)


##### Multiple-row insert

This section describes how to insert three rows of data into `nested_data_java` table.
The class [`MultipleRowInsert`](src/main/java/com/mapr/hiveconnector/examples/inserting/MultipleRowInsert.java) demonstrates this in two methods:
- by using a `dummy` table;
- by using a `SELECT` statement;


In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.inserting.MultipleRowInsert
```

>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#multiple-row-insert)


##### Overwriting data

The class [`Overwriting`](src/main/java/com/mapr/hiveconnector/examples/inserting/overwriting/Overwriting.java) demonstrates how to use 
the `INSERT` statement on a dummy table to overwrite one or more complete rows in `nested_data_java`.

In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.inserting.overwriting.Overwriting  
```

>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#overwriting-data)


#### Updating statements

This section describes how to use the `UPDATE` statement to update primitive, complex, and complex nested data types in MapR DB JSON tables, 
using the Hive connector from Java.

##### Update primitive data types

The class [`UpdatePrimitiveDataTypes`](src/main/java/com/mapr/hiveconnector/examples/updating/UpdatePrimitiveDataTypes.java) demonstrates 
how to update primitive data types in MapR Database JSON tables. 

In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.updating.UpdatePrimitiveDataTypes
```

>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#update-primitive-data-types)


##### Update complex data types

The class [`UpdateComplexDataTypes`](src/main/java/com/mapr/hiveconnector/examples/updating/UpdateComplexDataTypes.java) demonstrates 
how to update complex data types in MapR Database JSON tables. 

In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.updating.UpdateComplexDataTypes 
```

>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#update-complex-data-types)


##### Update complex nested data types

The class [`UpdateComplexNestedDataTypes`](src/main/java/com/mapr/hiveconnector/examples/updating/UpdateComplexNestedDataTypes.java) demonstrates 
how to update complex nested data types in MapR Database JSON tables. 

In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.updating.UpdateComplexNestedDataTypes
```

>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#update-complex-nested-data-types)


##### Update statement limitations

This section describes the features that the `UPDATE` statement does not support.

The `UPDATE` statement has the following known limitations:
-   The `UPDATE` statement is fully supported only for primitive data types (see [Connecting to MapR Database](https://mapr.com/docs/61/Hive/ConnectingToMapR-DB.html)).
-   The `UPDATE` statement is partly supported for complex data types; you can replace only the whole value of a complex type with new a value.
-   You cannot update the maprdb.column.id value.

#### Merging statements

This section describes how to use the `MERGE` statement to efficiently perform record-level `INSERT` and `UPDATE` operations within Hive tables from Java.


##### Simple merge.maprdb.column.id is the join key


The class [`MergeWhenIdIsJoinKey`](src/main/java/com/mapr/hiveconnector/examples/merging/MergeWhenIdIsJoinKey.java) demonstrates 
how to merge two tables joined by key. 

In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.merging.MergeWhenIdIsJoinKey
```

*The age column is updated and a new `id` column is inserted.*


##### Multiple source rows match a given target row (cardinality violation)

The class [`MergeMultipleRowsMatch`](src/main/java/com/mapr/hiveconnector/examples/merging/MergeMultipleRowsMatch.java) demonstrates 
how to merge two tables which have a few matched rows. 

In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.merging.MergeMultipleRowsMatch 
```

##### Merge on mixed data types

The class [`MergeMixedDataTypes`](src/main/java/com/mapr/hiveconnector/examples/merging/MergeMixedDataTypes.java) demonstrates 
how to merge two tables which have mixed data types, such as arrays, maps, and structures.

In a terminal window type the following command:

```
$ java -cp mapr-db_hive_connector-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.hiveconnector.examples.merging.MergeMixedDataTypes
```

>It is important to note that you cannot update only a part of a complex structure field. You cannot update only the age field in the structure. You can only replace all values of the structure with new ones. See [Understanding the UPDATE Statement](https://mapr.com/docs/61/Hive/UPDATEStatementForHive-mapr-dbJSONtables.html) for details. 
For example, you have a structure stored as one field in a Hive table:

```sh
{"name":"Johnson","surname":"Fall","age":23,"gender":"MALE"}
```

>To see how to do this operation using Hive CLI click the [link](001-hive-connector-cli.md#merging-statements)


#### Merge statement limitations

**Simple merge.maprdb.column.id is not the join key**
Merging when merge.maprdb.column is not the join key is not recommended.

**Deleting while merging**
Deletions are not supported in a `MERGE` statement.

**Merge into partitioned MapR Database JSON tables**
Partitioned MapR Database JSON tables are not supported.


### Run the application from the IDE

> This is an Optional.
 
The following steps are documented for MapR cluster where the secured mode has not been enabled.
This function requires the MapR Client must be configured on the host.

1. Create a file  `/opt/mapr/conf/mapr-clusters.conf`

Add the following configuration of your cluster in the file:

```
my.cluster.com secure=false mapr60:7222
```

Where:

* `my.cluster.com` is the name of your cluster
* `secure=false` specifies that the cluster secure mode is not enabled
* `mapr60:7222` is the host and port of the CLDB.


**2- Create a user on your cluster**

>Note : The following step is a known limitation of MapR 6.0 Beta

In addition to this, you also need to create a user with the same login to allow the MapR-DB Query Service powered by Drill.

For example if your desktop user is "jdoe" with the id 501, create the user on your cluster nodes using for example:

```
# useradd -u 501 jdoe
```

**3- [Clone and build the project](#clone-and-build-the-project)**

**4- Navigate to the interested class and run the main method in a class**