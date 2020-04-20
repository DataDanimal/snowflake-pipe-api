use database <SNKFLK_QUAL>_db;
use warehouse <SNKFLK_QUAL>_wh;
use role sysadmin;

show stages;


/******************************************************
**
** Part 1 - Create stages
**
*******************************************************/

-- Step 1 - Create stage schema
create or replace schema pipe_api_stg with managed access;

use schema pipe_api_stg;

-- Step 2 - Create file formats

create or replace file format ff_json_gz
 type = 'JSON'
 compression = gzip;
;

create or replace file format ff_tsv_gz
  type = csv
  field_delimiter = '\t'
  skip_header = 1
  null_if = ('NULL', 'null')
  empty_field_as_null = true
  compression = gzip;

-- Step 3 create external stage for data export
CREATE or replace stage ext_stg_export
  storage_integration = <SNKFLK_QUAL>_stor_int
  url = '<AWS_S3_URI>'
  ;

/******************************************************
**
** Part 2 - Create  Pipe for JSON weather data
**
*******************************************************/
use schema pipe_api_stg;

-- Step 1 create external stage for data ingest

CREATE or replace stage ext_stg_ingest
  storage_integration = <SNKFLK_QUAL>_stor_int
  url = '<AWS_S3_URI>>'
  ;

-- Step 2 - create sequence and table
create or replace sequence seq_weather_msg_key start with 1 increment by 1;

create or replace table daily_16_total_json
(weather_msg_key number default seq_weather_msg_key.nextval, 
       daily16_var VARIANT,
       daily16_ts timestamp,
       FILENAME string,
       FILE_ROW_NUMBER number,
       created_by string default current_user,
      CREATED_TIMESTAMP    TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP())
;

-- Step 2b - create sequence and table for TSV
create or replace sequence seq_weather_tsv_key start with 1 increment by 1;


create or replace table daily_16_total_tsv
(   weather_tsv_key number default seq_weather_tsv_key.nextval, 
	CITY_ID NUMBER(38,0),
	CITY VARCHAR(255),
	COUNTRY VARCHAR(255),
	LAT FLOAT,
	LON FLOAT,
	TIME TIMESTAMP_NTZ(9),
	CLOUDS NUMBER(38,0),
	DEG NUMBER(38,0),
	DT TIMESTAMP_NTZ(9),
	HUMIDITY NUMBER(38,0),
	PRESSURE FLOAT,
	UVI FLOAT,
	SPEED FLOAT,
	TEMP_DAY FLOAT,
	TEMP_EVE FLOAT,
	TEMP_MAX FLOAT,
	TEMP_MIN FLOAT,
	TEMP_MORN FLOAT,
	TEMP_NIGHT FLOAT,
	WEATHER_ID NUMBER(38,0),
	WEATHER_DESCRIPTION VARCHAR(1000),
	WEATHER_MAIN VARCHAR(255),
	WEATHER_ICON VARCHAR(255),
	T TIMESTAMP_NTZ(9),
       FILENAME string,
       FILE_ROW_NUMBER number,
       created_by string default current_user,
      CREATED_TIMESTAMP    TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP())
;

-- Step 3 - create pipe - JSON format
drop pipe if exists pipe_weather_daily_16_api_json_in; 
create or replace pipe pipe_weather_daily_16_api_json_in
 auto_ingest=false as
 copy into daily_16_total_json (daily16_var, daily16_ts, FILENAME, FILE_ROW_NUMBER)
FROM
   (
    SELECT  to_object(w.$1) daily16_var, to_object(w.$1):time::timestamp daily16_ts, METADATA$FILENAME fname, METADATA$FILE_ROW_NUMBER frownum
   FROM @ext_stg_ingest/weather/ w)
   FILE_FORMAT = (FORMAT_NAME='ff_json_gz')
;

-- Step 3b - create pipe - TSV format
drop pipe if exists pipe_weather_daily_16_api_tsv_in; 
create or replace pipe pipe_weather_daily_16_api_tsv_in
 auto_ingest=false as
copy into daily_16_total_tsv (CITY_ID ,
	CITY  ,
	COUNTRY  ,
	LAT  ,
	LON  ,
	TIME  ,
	CLOUDS  ,
	DEG  ,
	DT  ,
	HUMIDITY  ,
	PRESSURE  ,
	UVI  ,
	SPEED  ,
	TEMP_DAY  ,
	TEMP_EVE  ,
	TEMP_MAX  ,
	TEMP_MIN  ,
	TEMP_MORN  ,
	TEMP_NIGHT  ,
	WEATHER_ID  ,
	WEATHER_DESCRIPTION  ,
	WEATHER_MAIN ,
	WEATHER_ICON  ,
	T  ,
       FILENAME  ,
       FILE_ROW_NUMBER)
FROM
   (
    SELECT  w.$1,
w.$2,
w.$3,
w.$4,
w.$5,
w.$6,
w.$7,
w.$8,
w.$9,
w.$10,
w.$11,
w.$12,
w.$13,
w.$14,
w.$15,
w.$16,
w.$17,
w.$18,
w.$19,
w.$20,
w.$21,
w.$22,
w.$23,
w.$24, METADATA$FILENAME fname, METADATA$FILE_ROW_NUMBER frownum
   FROM @ext_stg_ingest/weather/ w)
   FILE_FORMAT = (FORMAT_NAME='ff_tsv_gz')
;



/*************************************
Check pipe status
*************************************/
/*select SYSTEM$PIPE_STATUS( upper('pipe_weather_daily_16_api_json_in') )
;
select SYSTEM$PIPE_STATUS( upper('pipe_weather_daily_16_api_tsv_in') )
;
select count(1) from daily_16_total_tsv ;
select * from daily_16_total_json limit 1000;

select s.last_load_time t, s.PIPE_RECEIVED_TIME,  s.* --status, count(1)
from table(information_schema.copy_history(table_name=>'daily_16_total_json',
                                           start_time=> dateadd(hours, -24, current_timestamp()))) s
order by t desc
;

use role accountadmin;


select *
  from table(information_schema.pipe_usage_history(
    date_range_start=>dateadd('hour',-24,current_timestamp()),
    pipe_name=> 'pipe_weather_daily_16_auto_in'))
;

select * from table(information_schema.validate_pipe_load(
  pipe_name=>'<SNKFLK_QUAL>_db.pipe_api_stg.pipe_weather_daily_16_auto_in',
  start_time=>dateadd(hour, -12, current_timestamp())));
*/
