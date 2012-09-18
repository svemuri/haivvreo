DROP TABLE tab1;

CREATE TABLE tab1(string1 STRING,
                             int1 INT,
                             tinyint1 TINYINT,
                             smallint1 SMALLINT,
                             bigint1 BIGINT,
                             boolean1 BOOLEAN,
                             float1 FLOAT,
                             double1 DOUBLE,
                             list1 ARRAY<STRING>,
                             map1 MAP<STRING,INT>,
                             struct1 STRUCT<sint:INT,sboolean:BOOLEAN,sstring:STRING>,
                             union1 uniontype<FLOAT, BOOLEAN, STRING>,
                             enum1 STRING,
                             nullableint INT,
                             bytes1 ARRAY<TINYINT>,
                             fixed1 ARRAY<TINYINT>)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY ':' MAP KEYS TERMINATED BY '#' LINES TERMINATED BY '\n'
 STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'tab1.csv' INTO TABLE tab1;

select * from tab1;

DROP TABLE tab1avro;

CREATE TABLE tab1avro(notused INT)
  ROW FORMAT SERDE
  'com.linkedin.haivvreo.AvroSerDe'
  WITH SERDEPROPERTIES (
    'schema.url'='hdfs:///schemas/tab1.avsc')
  STORED as INPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerInputFormat'
  OUTPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerOutputFormat';

insert overwrite table tab1avro select * from tab1;
select * from tab1avro;

DROP TABLE tab2avro;
CREATE TABLE tab2avro(notused INT)
  ROW FORMAT SERDE
  'com.linkedin.haivvreo.AvroSerDe'
  WITH SERDEPROPERTIES (
    'schema.url'='hdfs:///schemas//tab1.avsc')
  STORED as INPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerInputFormat'
  OUTPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerOutputFormat';

drop table tab2;
CREATE TABLE tab2(string1 STRING,
                             int1 INT,
                             tinyint1 TINYINT,
                             smallint1 SMALLINT,
                             bigint1 BIGINT,
                             boolean1 BOOLEAN,
                             float1 FLOAT,
                             double1 DOUBLE,
                             list1 ARRAY<STRING>,
                             map1 MAP<STRING,INT>,
                             struct1 STRUCT<sint:INT,sboolean:BOOLEAN,sstring:STRING>,
                             union1 uniontype<FLOAT, BOOLEAN, STRING>,
                             enum1 STRING,
                             nullableint INT,
                             bytes1 ARRAY<TINYINT>,
                             fixed1 ARRAY<TINYINT>)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY ':' MAP KEYS TERMINATED BY '#' LINES TERMINATED BY '\n'
 STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'tab2.csv' INTO TABLE tab2;
insert overwrite table tab2avro select * from tab2;
select * from tab2avro;

select tab1avro.string1, tab2avro.string1 from tab1avro JOIN tab2avro on tab1avro.int1 = tab2avro.int1;

select SQ.foo from (select string1 as foo from tab1avro) SQ;
select SQ.foo from (select int1 as foo from tab1avro) SQ;




