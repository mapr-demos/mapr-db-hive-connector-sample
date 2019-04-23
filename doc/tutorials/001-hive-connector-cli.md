# CLI examples for using Hive MapR Database JSON Connector
The examples to discover the key features of the Hive connector for MapR Database JSON table using Hive CLI. 

## Contents
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




### Creating tables
This section describes how to create tables in MapR DB JSON tables, using the Hive connector. The Hive connector supports the creation of MapR Database based Hive tables. 

##### Creating a MapR DB JSON table and Hive table using Hive
1. To create a table, first run `hive` and create database `demo`:
```
$ hive
hive> CREATE DATABASE demo;
```

2. To create a MapR DB JSON table and Hive table using hive run the command similar to the following:
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

3. Verifying that tables are created in Hive:

```
hive> show tables;
primitive_types
```

and in Mapr DB also. Using `maprcli table info -path /some-table-path -json`:

<details> 
  <summary>$ maprcli table info -path /primitive_types -json</summary>
  
```
{
	"timestamp":1554290132542,
	"timeofday":"2019-04-03 11:15:32.542 GMT+0000 AM",
	"status":"OK",
	"total":1,
	"data":[
		{
			"path":"/primitive_types",
			"numregions":1,
			"totallogicalsize":163840,
			"totalphysicalsize":98304,
			"totalcopypendingsize":0,
			"totalrows":2,
			"totalnumberofspills":2,
			"totalnumberofsegments":1,
			"autosplit":true,
			"bulkload":false,
			"tabletype":"json",
			"regionsizemb":4096,
			"audit":false,
			"metricsinterval":60,
			"maxvalueszinmemindex":100,
			"adminaccessperm":"u:mapr",
			"createrenamefamilyperm":"u:mapr",
			"bulkloadperm":"u:mapr",
			"indexperm":"u:mapr",
			"packperm":"u:mapr",
			"deletefamilyperm":"u:mapr",
			"replperm":"u:mapr",
			"splitmergeperm":"u:mapr",
			"defaultcompressionperm":"u:mapr",
			"defaultmemoryperm":"u:mapr",
			"defaultreadperm":"u:mapr",
			"defaulttraverseperm":"u:mapr",
			"defaultwriteperm":"u:mapr",
			"uuid":"c6922f90-8dc8-ae59-b015-09573aa25c00"
		}
	]
}
```

</details>

<br/>
4. Inserting the data into table `primitive_types`

```
hive> INSERT INTO TABLE primitive_types VALUES ('1', true, 124.14, '2017-11-29', 9192.12, 
214566190, 'text', '2017-03-17 00:14:13', 125, 9223372036854775806, 23434, "binary string");
```

Verifying that the data are inserted in Hive and Mapr DB.

```
hive> SELECT * FROM primitive_types;
1 true  124.14  2017-11-29  9192.12 214566190 text  2017-03-17 00:14:13 125 9223372036854775806 23434 binary string

$ mapr dbshell
maprdb mapr:> find /primitive_types
{"_id":"1","bi":{"$numberLong":9223372036854775806},"bin":{"$binary":"YmluYXJ5IHN0cmluZwAAAAAAAA=="},"bo":true,"d":124.14,"da":{"$dateDay":"2017-11-29"},"f":{"$numberFloat":9192.12},"i":{"$numberInt":214566190},"s":"text","si":{"$numberShort":23434},"ti":{"$numberByte":125},"ts":{"$date":"2017-03-17T00:14:13.000Z"}}
```

##### Creating a Hive table that exists on MapR DB JSON table

To create a Hive table over existing MapR Database JSON table need to use `EXTERNAL` keyword:

```
hive> CREATE EXTERNAL TABLE primitive_types (field type) 
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' 
TBLPROPERTIES("maprdb.table.name" = "/primitive_types","maprdb.column.id" = "id"); 
```

>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#creating-tables)


### Showing all tables in the database
This section describes how we can get the list of all tables using Hive CLI:

```
hive> show tables;
primitive_types
```

>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#showing-all-tables-in-the-database)

### Loading data to MapR DB using the Hive connector

This section describes how to load data in MapR DB JSON tables, using the Hive connector from local file system and MaprFs.
We will load data stored located in `/dataset/artists.json` file. 

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

1. To parce JSON table for our example, we will use JSON SerDe (serializer/deserializer).
   We have to add SerDe jar as a resource to the class path. If we do not explicitly bundle SerDe jar for JSON we get RuntimeException as below.
   
   <details> 
   
     <summary>RuntimeException caused by JsonSerDe not found</summary>
     
     ```
   Error: java.lang.RuntimeException: Error in configuring object
   ...
   Caused by: java.lang.reflect.InvocationTargetException
   ...
   Caused by: java.lang.RuntimeException: Error in configuring object
   ...
   Caused by: java.lang.reflect.InvocationTargetException
   ...
   Caused by: java.lang.RuntimeException: Map operator initialization failed
   ...
   Caused by: org.apache.hadoop.hive.ql.metadata.HiveException: java.lang.ClassNotFoundException: Class org.apache.hive.hcatalog.data.JsonSerDe not found
   ...
   Caused by: java.lang.ClassNotFoundException: Class org.apache.hive.hcatalog.data.JsonSerDe not found
     ```
   
   </details>  



Add SerDe JAR for JSON.
```
hive> add jar /opt/mapr/hive/hive-2.3/hcatalog/share/hcatalog/hive-hcatalog-core-2.3.3-mapr-1901.jar;
```

2. Creating an intermediate table.
>If there is a key in the JSON file that starts with "_" (for example, "_id"), then treat the names as literals upon creating the schema and query using the same literal syntax. For example, specify `_id` string without any special serde properties. Then in the query, use select `_id` from sometable;. Alternatively, you can use 'org.openx.data.jsonserde.JsonSerDe' and add WITH SERDEPROPERTIES ("mapping.id" = "_id" ) to your table definition.

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
3. Loading data in `artists_load` table.

*To load data from local file system:*
```
LOAD DATA LOCAL INPATH '/tmp/dataset/artists.json' OVERWRITE INTO TABLE artists_load;
```
*To load data from MaprFs:*
```
LOAD DATA INPATH '/tmp/artists.json' OVERWRITE INTO TABLE artists_load;
```

4. Creating MapR DB table in Hive.
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

5. Inserting data through `artists_load` table.
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

>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#loading-data-to-mapr-db-using-the-hive-connector)


#### Querying statements

This section describes how to use the `SELECT` statement within MapR DB JSON tables, using the Hive connector. 
To demonstrate it let's use the table `artists` created in the previous section.


##### Primitive data types

> As our JSON file contains a key that starts with "_" ("_id"), then in query need use the same literal syntax. In the above, it would look like `_id` string without any special serde properties for it. 
```
hive> select `_id`, name, rating, area from artists limit 4;
00034ede-a1f1-4219-be39-02f36853373e  O Rappa        2.5588235294117645  Brazil
0003fd17-b083-41fe-83a9-d550bd4f00a1  安倍なつみ       2.5208333333333335  Japan
0004537a-4b12-43eb-a023-04009e738d2e  Ultra Naté     2.769230769230769   States
0013bcdd-fe35-4c9f-ac41-4b41b9000e17  Siobhan Magnus 2.9107142857142856  States
```
>Alternatively, use `org.openx.data.jsonserde.JsonSerDe` and add `WITH SERDEPROPERTIES ("mapping.id" = "_id" )` to your table definition.

>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#primitive-data-types)

##### Complex data type

The `SELECT` statement with complex data types in MapR DB JSON tables, using the Hive connector. The query below gets 5 artist's name and the second album name which is not NULL.
```
hive> select name, albums.name[2] from artists where albums.name[2] IS NOT NULL limit 4; 
Laura McCormick             Wild Horses
Keith Zizza Caesar IV:      Original Game Music Score
The Real McKenzies          The Real McKenzies
Melbourne Ska Orchestra     The Best Things in Life Are Free
```

>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#complex-data-type)

##### Complex nested structures

The `SELECT` statement with nested data types in MapR DB JSON tables, using the Hive connector. To demonstrate this function let's create complex_nested_data_type table and fill with data. Dont forget to add SerDe jar.
```
add jar /opt/mapr/hive/hive-2.3/hcatalog/share/hcatalog/hive-hcatalog-core-2.3.3-mapr-1901.jar;

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


```sh
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

>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#complex-nested-structures)


#### Inserting statements
This section describes how to use the `INSERT INTO` statement to insert or overwrite rows in nested MapR DB JSON tables, using the Hive connector.


##### Single-row insert
The `INSERT INTO` statement to insert a single table row into a nested MapR DB table. 
To demonstrate this function let's create the nested data table.


```sh
hive> CREATE TABLE nested_data ( 
        entry STRING, 
        num INT, 
        postal_addresses MAP <STRING, 
        struct <USER_ID:STRING,ADDRESS:STRING,ZIP:STRING,COUNTRY:STRING>> ) 
    stored BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' tblproperties ( 
    "maprdb.table.name" = "/nested_data",  "maprdb.column.id" = "entry" );
```


1. The first insertion method by using a `dummy` table:
```sh
hive> WITH dummy_table AS (SELECT '001' AS KEY, '1' AS num,
        MAP ('Adam',
            Named_struct ('user_id', '1', 'address', '3205 Woodlake ct', 'zip', '45040', 'country', 'Usa'),
           'Wilfred',
           Named_struct ('user_id', '2', 'address', '777 Brockton Avenue', 'zip', '34000', 'country', 'Ita')) AS postal_addresses)
INSERT INTO nested_data
SELECT * FROM dummy_table;
```

2. The second insertion method by using a `SELECT` statement:
```sh
hive> INSERT INTO TABLE nested_data 
        SELECT '002',
        '2',
        MAP ('Bill',
            Named_struct ('user_id', '1', 'address', '328 Virginia Ave', 'zip', '54956', 'country', 'Bol'),
            'Stiv',
            Named_struct ('user_id', '2', 'address', 'Schererville', 'zip', '46375', 'country', 'Efi'));
```
<details> 
  <summary>Verifying that the data is inserted in both Hive and MapR Database JSON tables.</summary>
  
```sh
hive> SELECT * FROM nested_data;
001	1	{"Adam":{"user_id":"1","address":"3205 Woodlake ct","zip":"45040","country":"Usa"},"Wilfred":{"user_id":"2","address":"777 Brockton Avenue","zip":"34000","country":"Ita"}}
002	2	{"Bill":{"user_id":"1","address":"328 Virginia Ave","zip":"54956","country":"Bol"},"Stiv":{"user_id":"2","address":"Schererville","zip":"46375","country":"Efi"}}

$ mapr dbshell
maprdb mapr:> find /nested_data
{"_id":"001","num":{"$numberInt":1},"postal_addresses":{"Adam":{"address":"3205 Woodlake ct","country":"Usa","user_id":"1","zip":"45040"},"Wilfred":{"address":"777 Brockton Avenue","country":"Ita","user_id":"2","zip":"34000"}}}
{"_id":"002","num":{"$numberInt":2},"postal_addresses":{"Bill":{"address":"328 Virginia Ave","country":"Bol","user_id":"1","zip":"54956"},"Stiv":{"address":"Schererville","country":"Efi","user_id":"2","zip":"46375"}}}
```

</details> 


>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#single-row-insert)


##### Multiple-row insert

This section describes how to insert three rows of data into `nested_data` table.
1. The first insertion method by using a `dummy` table:

```sh
    hive> WITH dummy_table AS (SELECT '003' AS KEY,
              '3' AS num,
              MAP ('Rony',
                   Named_struct ('user_id', '1', 'address', '4333 Backer str', 'zip', '12311', 'country', 'Hun')) AS postal_addresses
       UNION ALL SELECT '004' AS KEY,
                        '4' AS num,
                        MAP ('Ivan',
                             Named_struct ('user_id', '1', 'address', '833 Bridle Avenue', 'zip', '95111', 'country', 'CA')) AS postal_addresses
       UNION ALL SELECT '005' AS KEY,
                        '5' AS num, 
                        MAP ('Ivan',
                             Named_struct ('user_id', '1', 'address', '664 Devon Ave', 'zip', '92021', 'country', 'Tog')) AS postal_addresses)
    INSERT INTO nested_data SELECT * FROM dummy_table;
```

2. The second insertion method by using a `SELECT` statement:
```sh
hive> INSERT INTO TABLE nested_data
    SELECT '006',
           '6',
           MAP ('Rony',
                Named_struct ('user_id', '1', 'address', '150 National City', 'zip', '91950', 'country', 'Hun'))
    UNION ALL
    SELECT '007',
           '7',
           MAP ('Tomason',
                Named_struct ('user_id', '1', 'address', '272 Ocean Circle' , 'zip', '92801', 'country', 'CA'))
    UNION ALL
    SELECT '008',
           '8',
           MAP ('Davin',
                Named_struct ('user_id', '1', 'address', '81 Augusta Ave', 'zip', '93905', 'country', 'CA'));
```
<details> 
  <summary>Verifying that the data is inserted in both Hive and MapR Database JSON tables.</summary>
  
```sh
hive> SELECT * FROM nested_data WHERE entry > '002' ;
003	3	{"Rony":{"user_id":"1","address":"4333 Backer str","zip":"12311","country":"Hun"}}
004	4	{"Ivan":{"user_id":"1","address":"833 Bridle Avenue","zip":"95111","country":"CA"}}
005	5	{"Ivan":{"user_id":"1","address":"664 Devon Ave","zip":"92021","country":"Tog"}}
006	6	{"Rony":{"user_id":"1","address":"150 National City","zip":"91950","country":"Hun"}}
007	7	{"Tomason":{"user_id":"1","address":"272 Ocean Circle","zip":"92801","country":"CA"}}
008	8	{"Davin":{"user_id":"1","address":"81 Augusta Ave","zip":"93905","country":"CA"}}

$ mapr dbshell
maprdb mapr:> find /nested_data
{"_id":"001","num":{"$numberInt":1},"postal_addresses":{"Adam":{"address":"3205 Woodlake ct","country":"Usa","user_id":"1","zip":"45040"},"Wilfred":{"address":"777 Brockton Avenue","country":"Ita","user_id":"2","zip":"34000"}}}
{"_id":"002","num":{"$numberInt":2},"postal_addresses":{"Bill":{"address":"328 Virginia Ave","country":"Bol","user_id":"1","zip":"54956"},"Stiv":{"address":"Schererville","country":"Efi","user_id":"2","zip":"46375"}}}
{"_id":"003","num":{"$numberInt":3},"postal_addresses":{"Rony":{"address":"4333 Backer str","country":"Hun","user_id":"1","zip":"12311"}}}
{"_id":"004","num":{"$numberInt":4},"postal_addresses":{"Ivan":{"address":"833 Bridle Avenue","country":"CA","user_id":"1","zip":"95111"}}}
{"_id":"005","num":{"$numberInt":5},"postal_addresses":{"Ivan":{"address":"664 Devon Ave","country":"Tog","user_id":"1","zip":"92021"}}}
{"_id":"006","num":{"$numberInt":6},"postal_addresses":{"Rony":{"address":"150 National City","country":"Hun","user_id":"1","zip":"91950"}}}
{"_id":"007","num":{"$numberInt":7},"postal_addresses":{"Tomason":{"address":"272 Ocean Circle","country":"CA","user_id":"1","zip":"92801"}}}
{"_id":"008","num":{"$numberInt":8},"postal_addresses":{"Davin":{"address":"81 Augusta Ave","country":"CA","user_id":"1","zip":"93905"}}}
```

</details> 

>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#multiple-row-insert)

##### Overwriting data

This section describes how to use the INSERT statement on a dummy table to overwrite one or more complete rows. Let's overwrite the first row in `nested_data` (001) with new values.

```sh
hive> WITH dummy_table AS
    (SELECT '001' AS KEY,
    '1' AS num,
    MAP ('newAdam',
    Named_struct ('user_id', '1', 'address', 'newAdress', 'zip', 'newZip', 'country', 'newCountry')) AS postal_addresses)
    INSERT INTO nested_data SELECT * FROM dummy_table;
```

<details> 

  <summary>Verifying that the data is overwritten in both Hive and MapR Database JSON tables.</summary>
  
```
hive> SELECT * FROM nested_data WHERE entry = '001';
001	1	{"newAdam":{"user_id":"1","address":"newAdress","zip":"newZip","country":"newCountry"}}

$ mapr dbshell
maprdb mapr:> findbyid /nested_data --id 001
{"_id":"001","num":{"$numberInt":1},"postal_addresses":{"newAdam":{"address":"newAdress","country":"newCountry","user_id":"1","zip":"newZip"}}}
```

</details> 

Also we can overwrite a few. Let's overwrite 003 and 004 rows in `nested_data` with new values.

```sh
hive> WITH dummy_table AS (
    SELECT '003' AS KEY,
    '3' AS num,
    MAP ('newName1',
    Named_struct ('user_id', '1', 'address', 'newAdress1', 'zip', 'newZip1', 'country', 'newCountry1')) AS postal_addresses
    UNION ALL
    SELECT '004' AS KEY,
    '4' AS num,
    MAP ('newName2',
    Named_struct ('user_id', '1', 'address', 'newAdress2', 'zip', 'newZip2', 'country', 'newCountry2')) AS postal_addresses)
    INSERT INTO nested_data SELECT * FROM dummy_table;
```

<details> 

  <summary>Verifying that the data is overwritten in both Hive and MapR Database JSON tables.</summary>
  
```sh
hive> SELECT * FROM nested_data WHERE entry IN ('003', '004');
003	3	{"newName1":{"user_id":"1","address":"newAdress1","zip":"newZip1","country":"newCountry1"}}
004	4	{"newName2":{"user_id":"1","address":"newAdress2","zip":"newZip2","country":"newCountry2"}}

$ mapr dbshell
maprdb mapr:> findbyid /nested_data --id 003
{"_id":"003","num":{"$numberInt":3},"postal_addresses":{"newName1":{"address":"newAdress1","country":"newCountry1","user_id":"1","zip":"newZip1"}}}
maprdb mapr:> findbyid /nested_data --id 004
{"_id":"004","num":{"$numberInt":4},"postal_addresses":{"newName2":{"address":"newAdress2","country":"newCountry2","user_id":"1","zip":"newZip2"}}}
```

</details> 

If you exclude columns both from the `SELECT` statement in your `INSERT` statement and from the table schema, the value of this column changes to NULL. To demonstrate this lets overwrite the first row in `nested_data` (001) with new values and overwrite the num column to NULL.

```sh
hive> WITH dummy_table AS
    (SELECT '001' AS KEY,
    MAP ('newAdam',
    Named_struct ('user_id', '1', 'address', 'newAdress', 'zip', 'newZip', 'country', 'newCountry')) AS postal_addresses)
    INSERT INTO nested_data (entry, postal_addresses)
    SELECT * FROM dummy_table;
```
<details> 
  <summary>Verifying that the data is overwritten in both Hive and MapR Database JSON tables.</summary>
  
```sh
hive> SELECT * FROM nested_data WHERE entry = '001';
001	NULL	{"newAdam":{"user_id":"1","address":"newAdress","zip":"newZip","country":"newCountry"}}

$ mapr dbshell
maprdb mapr:> findbyid /nested_data --id 001
{"_id":"001","postal_addresses":{"newAdam":{"address":"newAdress","country":"newCountry","user_id":"1","zip":"newZip"}}}
```

</details> 


>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#overwriting-data)


#### Updating statements

This section describes how to use the `UPDATE` statement to update primitive, complex, and complex nested data types in MapR DB JSON tables, using the Hive connector.

##### Update primitive data types

The `UPDATE` statement to update primitive data types in MapR Database JSON tables, using the Hive connector. Here we will use the `primitive_types` table created in the [Creating tables](#creating-tables) section. 

1. Use `SELECT` statement to see the data into table.

```sh
hive> SELECT * FROM primitive_types;
1	true	124.14	2017-11-29	9192.12	214566190	text	2017-03-17 00:14:13	125	9223372036854775806	23434	binary string
```

2. Run the `UPDATE` command on the table:

```sh
hive> UPDATE primitive_types SET 
    da = '2018-12-11',
    bo = FALSE,
    f = 91.777 WHERE doc_id = '1';
```

<details> 

  <summary>Verifying that the data is updated in both Hive and MapR Database JSON tables.</summary>
  
```sh
hive> SELECT * FROM primitive_types;
1	false	124.14	2018-12-11	91.777	214566190	text	2017-03-17 00:14:13	125	9223372036854775806	23434	binary string

$ mapr dbshell
maprdb mapr:> find /primitive_types
{"_id":"1","bi":{"$numberLong":9223372036854775806},"bin":{"$binary":"YmluYXJ5IHN0cmluZwAAAAAAAA=="},"bo":false,"d":124.14,"da":{"$dateDay":"2018-12-11"},"f":{"$numberFloat":91.777},"i":{"$numberInt":214566190},"s":"text","si":{"$numberShort":23434},"ti":{"$numberByte":125},"ts":{"$date":"2017-03-17T00:14:13.000Z"}}
```

</details> 

>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#update-primitive-data-types)

##### Update complex data types

The `UPDATE` statement to update complex data types in MapR Database JSON tables, using the Hive connector.

1. Create a MapR Database JSON table and a Hive table:

```sh
hive> CREATE TABLE complex_types (
  doc_id string, info MAP<STRING, INT>, pets ARRAY<STRING>,
  user_info STRUCT<name:STRING, surname:STRING, age:INT, gender:STRING>)
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler'
TBLPROPERTIES("maprdb.table.name" = "/complex_types","maprdb.column.id" = "doc_id");
```

2. Insert data into the table:

```sh
hive> INSERT INTO TABLE complex_types SELECT '1', map('age', 28), array('Cat', 'Cat', 'Cat'), 
named_struct('name', 'Santa', 'surname', 'Claus','age', 1000,'gender', 'MALE');
```

3. Run the `UPDATE` command on the table:

```sh
hive> UPDATE complex_types SET
    info = map('year', 32), pets = array('Dog', 'Cat', 'Pig'), 
    user_info = named_struct('name', 'Vasco', 'surname', 'da Gama','age', 558,'gender', 'MALE')
    WHERE doc_id = '1';
```

<details> 

  <summary>Verifying that the data is updated in both Hive and MapR Database JSON tables.</summary>
  
```sh
hive> SELECT * FROM complex_types;
1	{"year":32}	["Dog","Cat","Pig"]	{"name":"Vasco","surname":"da Gama","age":558,"gender":"MALE"}

$ mapr dbshell
maprdb mapr:> find /complex_types
{"_id":"1","info":{"year":{"$numberInt":32}},"pets":["Dog","Cat","Pig"],"user_info":{"age":{"$numberInt":558},"gender":"MALE","name":"Vasco","surname":"da Gama"}}
```

</details> 

>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#update-complex-data-types)

##### Update complex nested data types

The `UPDATE` statement to update complex nested data types in MapR Database JSON tables, using the Hive connector.

1. Creating a MapR DB JSON table and a Hive table using Hive:

```sh
hive> CREATE TABLE complex_nested_data ( 
          entry STRING, num INT, postal_addresses MAP <STRING, 
          struct <USER_ID:STRING,ADDRESS:STRING,ZIP:STRING,COUNTRY:STRING>> ) 
stored BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' tblproperties ( 
           "maprdb.table.name" = "/complex_nested_data", "maprdb.column.id" = "entry" );
```

2. Inserting data into the `complex_nested_data` table:

```sh
hive> INSERT INTO TABLE complex_nested_data
    SELECT '001', '1',
    MAP ( 'Bill',
    Named_struct ('user_id', '1', 'address', '3205 Woodlake ct', 'zip', '45040', 'country', 'USA'));
```

3. Running the `UPDATE` command on the table by updating the COUNTRY value in map(struct):

```
hive> UPDATE complex_nested_data
    SET postal_addresses = MAP ('Bill',
    Named_struct ('user_id', '1', 'address', '3205 Woodlake ct', 'zip', '45040', 'country', 'Hun'))
    WHERE entry = '001';
```

<details> 

  <summary>Verifying that the data is updated in both Hive and MapR Database JSON tables.</summary>
  
```sh
hive> SELECT * FROM complex_nested_data;
001	1	{"Bill":{"user_id":"1","address":"3205 Woodlake ct","zip":"45040","country":"Hun"}}

$ mapr dbshell
maprdb mapr:> find /complex_nested_data
{"_id":"001","num":{"$numberInt":1},"postal_addresses":{"Bill":{"address":"3205 Woodlake ct","country":"Hun","user_id":"1","zip":"45040"}}}
```

</details> 

>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#update-complex-nested-data-types)

##### Update statement limitations

This section describes the features that the `UPDATE` statement does not support.

The `UPDATE` statement has the following known limitations:
-   The `UPDATE` statement is fully supported only for primitive data types (see [Connecting to MapR Database](https://mapr.com/docs/61/Hive/ConnectingToMapR-DB.html)).
-   The `UPDATE` statement is partly supported for complex data types; you can replace only the whole value of a complex type with new a value.
-   You cannot update the maprdb.column.id value.

> Also keep in mind that if you update a document without `_id` specific `WHERE` clause, the HIVE will update all document.


<details> 

  <summary>Example update with no `_id` specified `WHERE` clause</summary>
  
```sh
hive> SELECT * FROM complex_types;
1	{"age":28}	["Cat","Cat","Cat"]	{"name":"Santa","surname":"Claus","age":1000,"gender":"MALE"}
2	{"age":8}	["Pet","Pet","Pet"]	{"name":"Santa1","surname":"Claus1","age":10,"gender":"FEMALE"}

hive> UPDATE complex_types SET
    info = map('year', 32), pets = array('Dog', 'Cat', 'Pig'), 
    user_info = named_struct('name', 'Vasco', 'surname', 'da Gama','age', 558,'gender', 'MALE');
    
hive> SELECT * FROM complex_types;
1	{"year":32}	["Dog","Cat","Pig"]	{"name":"Vasco","surname":"da Gama","age":558,"gender":"MALE"}
2	{"year":32}	["Dog","Cat","Pig"]	{"name":"Vasco","surname":"da Gama","age":558,"gender":"MALE"}
```

</details> 


#### Merging statements

This section describes how to use the `MERGE` statement to efficiently perform record-level `INSERT` and `UPDATE` operations within Hive tables.

##### Simple merge.maprdb.column.id is the join key

Consider merging the following example source and target tables:

Table customer_source

id      | first_name        | last_name | age
------- | ---------------- | ---------- | ---------:
001     | Dorothi          | Hogward    | 777
002     | Alex             | Bowee      | 777
088     | Robert           | Dowson     | 25

Table customer_db_json_target

id      | first_name        | last_name | age
------- | ---------------- | ---------- | ---------:
001     | John             | Smith      | 45
002     | Michael          | Watson     | 27
003     | Den              | Brown      | 33


<details> 
  <summary>1. Creating these tablets and fill them with the data.</summary>
  
```sh
hive> CREATE TABLE customer_source (
  id string, first_name string, last_name string, age int)
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler'
TBLPROPERTIES("maprdb.table.name" = "/customer_source","maprdb.column.id" = "id");

hive> CREATE TABLE customer_db_json_target (
  id string, first_name string, last_name string, age int)
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler'
TBLPROPERTIES("maprdb.table.name" = "/customer_db_json_target","maprdb.column.id" = "id");

hive> INSERT INTO TABLE customer_source VALUES ('001', 'Dorothi', 'Hogward', 7777);
hive> INSERT INTO TABLE customer_source VALUES ('002', 'Alex', 'Bowee', 7777);
hive> INSERT INTO TABLE customer_source VALUES ('088', 'Robert', 'Dowson', 25);

hive> INSERT INTO TABLE customer_db_json_target VALUES ('001', 'John', 'Smith', 45);
hive> INSERT INTO TABLE customer_db_json_target VALUES ('002', 'Michael', 'Watson', 27);
hive> INSERT INTO TABLE customer_db_json_target VALUES ('003', 'Den', 'Brown', 33);

hive> SELECT * FROM customer_source;
001	Dorothi	Hogward	7777
002	Alex	Bowee	7777
088	Robert	Dowson	25

hive> SELECT * FROM customer_db_json_target;
001	John	Smith	45
002	Michael	Watson	27
003	Den	Brown	33
```

</details>

2. Merge by using the following SQL-standard `MERGE` statement. To disable optimize Auto Join Conversion and avoid exceptions due to  merging need to set the hive command `Hive.auto.convert.join` in false.

```sh
hive> set hive.auto.convert.join=false;

hive> MERGE into customer_db_json_target trg USING customer_source src ON src.id = trg.id 
    WHEN MATCHED THEN UPDATE SET age = src.age 
    WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.first_name, src.last_name, src.age);
```
<details> 

  <summary>Verifying the target table in Hive and Mapr DB.</summary>
  
```sh
hive> SELECT * FROM customer_db_json_target;
001	John	Smith	7777
002	Michael	Watson	7777
003	Den	Brown	33
088	Robert	Dowson	25

$ mapr dbshell
maprdb mapr:> find /customer_db_json_target
{"_id":"001","age":{"$numberInt":7777},"first_name":"John","last_name":"Smith"}
{"_id":"002","age":{"$numberInt":7777},"first_name":"Michael","last_name":"Watson"}
{"_id":"003","age":{"$numberInt":33},"first_name":"Den","last_name":"Brown"}
{"_id":"088","age":{"$numberInt":25},"first_name":"Robert","last_name":"Dowson"}
```

</details> 

*The age column is updated and a new `id` column is inserted.*


##### Multiple source rows match a given target row (cardinality violation)

Consider merging the two `tables customer_db_json` and `customer_new`:


Table customer_db_json


id      | first_name        | last_name | age
------- | ---------------- | ---------- | ---------:
001	    |   John           |	Smith   |	45     |
002	    |   Michael        |	Watson  |	27     |
003	    |   Den            |	Brown   |	33     |


Table customer_new


id      | first_name        | last_name | age
------- | ---------------- | ---------- | ---------:
001     |	Dorothi        |	Hogward |	77     |
001     |	Dorothi        |	Hogward |	77     |
088     |	Robert         |	Dowson  |	25     |


<details> 

  <summary>1. Creating these tablets and fill them with the data.</summary>
  
```sh
hive> CREATE TABLE customer_db_json (
  id string, first_name string, last_name string, age int)
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler'
TBLPROPERTIES("maprdb.table.name" = "/customer_db_json","maprdb.column.id" = "id");

hive> CREATE TABLE customer_new (
  id string, first_name string, last_name string, age int)
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler'
TBLPROPERTIES("maprdb.table.name" = "/customer_new","maprdb.column.id" = "id");

hive> INSERT INTO TABLE customer_db_json VALUES ('001', 'John', 'Smith', 45);
hive> INSERT INTO TABLE customer_db_json VALUES ('002', 'Michael', 'Watson', 27);
hive> INSERT INTO TABLE customer_db_json VALUES ('003', 'Den', 'Brown', 33);

hive> INSERT INTO TABLE customer_new VALUES ('001', 'Dorothi', 'Hogward', 77);
hive> INSERT INTO TABLE customer_new VALUES ('001', 'Dorothi', 'Hogward', 77);
hive> INSERT INTO TABLE customer_new VALUES ('088', 'Robert', 'Dowson', 25);
```
</details> 

2. MERGE `customer_new` and `customer_db_json`. This example causes an exception because of duplicate values in the `id` column in the `customer_new table`. To avoid cardinality violation, `set hive.merge.cardinality.check=false`.
*But in this case the result is unpredictable because there is no rule which defines the order of duplicated data that will be inserted while using the `MERGE` statement.*

```sh
hive> MERGE INTO customer_db_json trg USING customer_new src ON src.id = trg.id 
    WHEN MATCHED THEN UPDATE SET first_name = src.first_name, last_name = src.last_name 
    WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.first_name, src.last_name, src.age);
```

<details> 

  <summary>Verifying the target table in Hive and Mapr DB.</summary>
  
```sh
hive> select * from customer_db_json;
001	Dorothi	Hogward	45
002	Michael	Watson	27
003	Den	Brown	33
088	Robert	Dowson	25

$ mapr dbshell
maprdb mapr:> find /customer_db_json
{"_id":"001","age":{"$numberInt":45},"first_name":"Dorothi","last_name":"Hogward"}
{"_id":"002","age":{"$numberInt":27},"first_name":"Michael","last_name":"Watson"}
{"_id":"003","age":{"$numberInt":33},"first_name":"Den","last_name":"Brown"}
{"_id":"088","age":{"$numberInt":25},"first_name":"Robert","last_name":"Dowson"}
```

</details> 

##### Merge on mixed data types

The merge operation also supports mixed data types, such as arrays, maps, and structures.
Consider two tables `mixed_types_source` and `mixed_types_target`:

Table mixed_types_source

doc_id  | user_info 
------- | ----------------:
1       |	{"name":"Brandon","surname":"Lee","age":31,"gender":"MALE"}|
2       |	{"name":"Johnson","surname":"Fall","age":23,"gender":"MALE"}|
3       |	{"name":"Mary","surname":"Dowson","age":11,"gender":"FEMALE"}|
4       |	{"name":"Paul","surname":"Rodgers","age":41,"gender":"MALE"}|

Table mixed_types_target

doc_id  | user_info 
------- | ----------------:
1       |	{"name":"Lexx","surname":"Comfuzer","age":31,"gender":"MALE"}|

<details> 

  <summary>1. Creating these tablets and fill them with the data.</summary>
  
```sh
hive> CREATE TABLE mixed_types_source (
  doc_id string, 
  user_info struct <name:STRING,surname:STRING,age:INT,gender:STRING>)
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler'
TBLPROPERTIES("maprdb.table.name" = "/mixed_types_source","maprdb.column.id" = "doc_id");

hive> CREATE TABLE mixed_types_target (
  doc_id string, 
  user_info struct <name:STRING,surname:STRING,age:INT,gender:STRING>)
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler'
TBLPROPERTIES("maprdb.table.name" = "/mixed_types_target","maprdb.column.id" = "doc_id");

hive> INSERT INTO TABLE mixed_types_source 
SELECT '1', named_struct('name', 'Brandon', 'surname', 'Lee', 'age', 31, 'gender', 'MALE')
UNION ALL
SELECT '2', named_struct ('name', 'Johnson', 'surname', 'Fall', 'age', 23, 'gender', 'MALE')
UNION ALL
SELECT '3', named_struct ('name', 'Mary', 'surname', 'Dowson', 'age', 11, 'gender', 'FEMALE')
UNION ALL
SELECT '4', named_struct ('name', 'Paul', 'surname', 'Rodgers', 'age', 41, 'gender', 'MALE');

hive> INSERT INTO TABLE mixed_types_target
SELECT '1', named_struct ('name', 'Lexx', 'surname', 'Comfuzer', 'age', 31, 'gender', 'MALE');

hive> select * from mixed_types_source;
1	{"name":"Brandon","surname":"Lee","age":31,"gender":"MALE"}
2	{"name":"Johnson","surname":"Fall","age":23,"gender":"MALE"}
3	{"name":"Mary","surname":"Dowson","age":11,"gender":"FEMALE"}
4	{"name":"Paul","surname":"Rodgers","age":41,"gender":"MALE"}

hive> SELECT * FROM mixed_types_target;
1	{"name":"Lexx","surname":"Comfuzer","age":31,"gender":"MALE"}
```

</details>

2. Merge `mixed_types_source` and `mixed_types_target` tables.

```sh
hive> MERGE INTO mixed_types_target trg USING mixed_types_source src ON src.doc_id = trg.doc_id 
WHEN MATCHED THEN UPDATE SET user_info = src.user_info 
WHEN NOT MATCHED THEN INSERT VALUES (src.doc_id, src.user_info);
```

<details> 

  <summary>Verifying the target table in Hive and Mapr DB.</summary>
  
```sh
hive> select * from mixed_types_target;
1	{"name":"Brandon","surname":"Lee","age":31,"gender":"MALE"}
2	{"name":"Johnson","surname":"Fall","age":23,"gender":"MALE"}
3	{"name":"Mary","surname":"Dowson","age":11,"gender":"FEMALE"}
4	{"name":"Paul","surname":"Rodgers","age":41,"gender":"MALE"}

$ mapr dbshell
maprdb mapr:> find /mixed_types_target
{"_id":"1","user_info":{"age":{"$numberInt":31},"gender":"MALE","name":"Brandon","surname":"Lee"}}
{"_id":"2","user_info":{"age":{"$numberInt":23},"gender":"MALE","name":"Johnson","surname":"Fall"}}
{"_id":"3","user_info":{"age":{"$numberInt":11},"gender":"FEMALE","name":"Mary","surname":"Dowson"}}
{"_id":"4","user_info":{"age":{"$numberInt":41},"gender":"MALE","name":"Paul","surname":"Rodgers"}}
```

</details> 

>It is important to note that you cannot update only a part of a complex structure field. You cannot update only the age field in the structure. You can only replace all values of the structure with new ones. See [Understanding the UPDATE Statement](https://mapr.com/docs/61/Hive/UPDATEStatementForHive-mapr-dbJSONtables.html) for details. 
For example, you have a structure stored as one field in a Hive table:

```sh
{"name":"Johnson","surname":"Fall","age":23,"gender":"MALE"}
```


>To see how to do this operation using Hive connector from Java click the [link](002-hive-connector-java.md#merging-statements)


#### Merge statement limitations

**Simple merge.maprdb.column.id is not the join key**
Merging when merge.maprdb.column is not the join key is not recommended.

**Deleting while merging**
Deletions are not supported in a `MERGE` statement.

**Merge into partitioned MapR Database JSON tables**
Partitioned MapR Database JSON tables are not supported.