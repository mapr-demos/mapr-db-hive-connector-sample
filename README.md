# Hive MapR Database JSON Connector Tutorial
This repository will contain examples to discover the key features of the Hive connector for MapR Database JSON table. 

## Contents
* [Prerequisites](#prerequisites)
* [Creating tables](#creating_tables)
* [Load data to MapR DB using the Hive connector](#load_data_to_MapR_DB_using_the_Hive_connector)
* [Querying statements](#querying_statements)
  - [Primitive data types](#primitive_data_types)
  - [Complex data type](#complex_data_type)
  - [Complex nested structures](#Complex_nested_structures)
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



### Prerequisites
* MapR Converged Data Platform 6.1 with Apache Hive and Apache Drill or [MapR Container for Developers](#https://mapr.com/docs/home/MapRContainerDevelopers/MapRContainerDevelopersOverview.html).
* JDK 8
* All datasets needed are located `/dataset`

### Creating tables
This section describes how to create tables in MapR DB JSON tables, using the Hive connector. The Hive connector supports the creation of MapR Database based Hive tables. 

##### Creating a MapR DB JSON table and Hive table using Hive
To create a table, first run hive and create database demo:
```
$ hive
hive> CREATE DATABASE demo;
```

To create a MapR DB JSON table and Hive table using hive run the command similar to the following:
```
hive> CREATE TABLE primitive_types (
  doc_id string,
  bo boolean,
  d double,
  da date,
  f float,
  i int,
  s string,
  ts timestamp,
  ti tinyint,
  bi bigint,
  si smallint,
  bin binary)
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler'
TBLPROPERTIES("maprdb.table.name" = "/primitive_types","maprdb.column.id" = "doc_id");
```

Verifying Hive and Mapr DB tables are created.
```
hive> show tables;
OK
primitive_types
```
```
$ hadoop fs -ls /
tr--------   - mapr mapr          3 2019-04-01 11:41 /primitive_types
```

Now insert the data into table `primitive_types`
```
hive> INSERT INTO TABLE primitive_types VALUES ('1', true, 124.14, '2017-11-29', 9192.12, 
214566190, 'text', '2017-03-17 00:14:13', 125, 9223372036854775806, 23434, "binary string");
```

Verifying that the data are inserted in Hive and Mapr DB.
```
hive> SELECT * FROM primitive_types;
1 true  124.14  2017-11-29  9192.12 214566190 text  2017-03-17 00:14:13 125 9223372036854775806 23434 binary string
```
```
maprdb mapr:> find /primitive_types
{"_id":"1","bi":{"$numberLong":9223372036854775806},"bin":{"$binary":"YmluYXJ5IHN0cmluZwAAAAAAAA=="},"bo":true,"d":124.14,"da":{"$dateDay":"2017-11-29"},"f":{"$numberFloat":9192.12},"i":{"$numberInt":214566190},"s":"text","si":{"$numberShort":23434},"ti":{"$numberByte":125},"ts":{"$date":"2017-03-17T00:14:13.000Z"}}
```

##### Creating a Hive table that exists on MapR DB JSON table
To create a Hive table over existing MapR Database JSON table need to use `EXTERNAL` keyword. For example:
```
hive> CREATE EXTERNAL TABLE primitive_types (field type) 
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' 
TBLPROPERTIES("maprdb.table.name" = "/primitive_types","maprdb.column.id" = "id"); 
```

#### Load data to MapR DB using the Hive connector
This section describes how to load data in MapR DB JSON tables, using the Hive connector from local file system and MaprFs.
You can create a JSON table on MapR DB and load data from JSON file to MapR DB using the connector. 
We will load data stored located in `/dataset/artists.json` file. 

#### artists
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

1. Add SerDe JAR for JSON.
For example:
```
hive> add jar /opt/mapr/hive/hive-2.3/hcatalog/share/hcatalog/hive-hcatalog-core-2.3.3-mapr-1901.jar;
```

2. Create intermediate table.
If there is a key in the JSON file that starts with "_" (for example, "_id"), then treat the names as literals upon creating the schema and query using the same literal syntax. For example, specify `_id` string without any special serde properties. Then in the query, use select `_id` from sometable;. Alternatively, you can use 'org.openx.data.jsonserde.JsonSerDe' and add WITH SERDEPROPERTIES ("mapping.id" = "_id" ) to your table definition.
```
CREATE EXTERNAL TABLE artists_load (
    `_id` string,
    albums array<struct<cover_image_url:string, id:string, name:string, slug:string>>,
    area string,
    deleted boolean,
    disambiguation_comment string,
    gender string,
    images_urls array<string>,
    mbid string,
    name string,
    profile_image_url string,
    rating double,
    slug_name string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' 
STORED AS TEXTFILE;
```
3. Load data in artists_load table.
To load data from local file system:
```
LOAD DATA LOCAL INPATH '/tmp/dataset/artists.json' OVERWRITE INTO TABLE artists_load;
```
To load data from MaprFs:
```
LOAD DATA INPATH '/tmp/artists.json' OVERWRITE INTO TABLE artists_load;
```

4. Create MapR DB table in Hive.
```
CREATE TABLE artists (
   `_id` string,
   albums array<struct<cover_image_url:string, id:string, name:string, slug:string>>,
   area string,
   deleted boolean,
   disambiguation_comment string,
   gender string,
   images_urls array<string>,
   mbid string,
   name string,
   profile_image_url string,
   rating double,
   slug_name string)
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' 
TBLPROPERTIES("maprdb.table.name" = "/artists","maprdb.column.id" = "_id"); 
```

5. Insert data through artists_load table.
```
INSERT INTO TABLE artists select 
   `_id` string,
   albums,
   area,
   deleted,
   disambiguation_comment,
   gender,
   images_urls,
   mbid,
   name,
   profile_image_url,
   rating,
   slug_name
from artists_load;
```

#### Querying statements
This section describes how to use the `SELECT` statement with in MapR DB JSON tables, using the Hive connector.
##### Primitive data types
As our JSON file contains a key that starts with "_" ("_id"), then in query need use the same literal syntax. In the above, it would look like `_id` string without any special serde properties for it. 
```
hive> select `_id`, name, rating, area from artists limit 4;
OK
00034ede-a1f1-4219-be39-02f36853373e  O Rappa        2.5588235294117645  Brazil
0003fd17-b083-41fe-83a9-d550bd4f00a1  安倍なつみ       2.5208333333333335  Japan
0004537a-4b12-43eb-a023-04009e738d2e  Ultra Naté     2.769230769230769   States
0013bcdd-fe35-4c9f-ac41-4b41b9000e17  Siobhan Magnus 2.9107142857142856  States
```
Alternatively, use org.openx.data.jsonserde.JsonSerDe and add WITH SERDEPROPERTIES ("mapping.id" = "_id" ) to your table definition.
##### Complex data type
The `SELECT` statement with complex data types in MapR DB JSON tables, using the Hive connector. The query below gets 5 artist's name and the second album name which is not NULL.
```
hive> select name, albums.name[2] from artists where albums.name[2] IS NOT NULL limit 4; 
OK
Laura McCormick             Wild Horses
Keith Zizza Caesar IV:      Original Game Music Score
The Real McKenzies          The Real McKenzies
Melbourne Ska Orchestra     The Best Things in Life Are Free
```
##### Complex nested structures
The `SELECT` statement with nested data types in MapR DB JSON tables, using the Hive connector. To demonstrate this function let's create complex_nested_data_type table and fill with data. Dont forget to add SerDe jar.
```
add jar /opt/mapr/hive/hive-2.3/hcatalog/share/hcatalog/hive-hcatalog-core-2.3.3-mapr-1901.jar;
```
```
hive> CREATE EXTERNAL TABLE json_nested_load (
    country string,
    languages array<string>,
    religions map<string,array<int>>)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE;
```
<details> 
  <summary>Complex nested structures source</summary>
```
{"country":"Switzerland","languages":["German","French","Italian"],"religions":{"catholic":[10,20],"protestant":[40,50]}}
```
</details>
```
hive> LOAD DATA LOCAL INPATH '/tmp/dataset/nested.json' OVERWRITE INTO TABLE json_nested_load;

CREATE TABLE json_nested (
    country string,
    languages array<string>,
    religions map<string,array<int>>)
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' 
TBLPROPERTIES("maprdb.table.name" = "/json_nested","maprdb.column.id" = "country"); 

hive> INSERT INTO TABLE json_nested select country, languages, religions from json_nested_load;

hive> select * from json_nested;
Switzerland ["German","French","Italian"] {"protestant":[40,50],"catholic":[10,20]}

hive> select languages[0] from json_nested;
German

hive> select religions['catholic'][0] from json_nested;
10
```
#### Inserting statements
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