/*************************************
Generate source files
*************************************/

-- step 4a: Export weather data as json files
-- one month, 4 cities

copy into @ext_stg_export/weather/daily_16_total_2018_jan_
from (select v
from sample_data.weather.daily_16_total
where t between to_timestamp('01/01/2017', 'MM/DD/YYYY') and
                dateadd('day', -1, dateadd('month', 1,     -- monthend
                to_timestamp('01/01/2017', 'MM/DD/YYYY')) )
  and v:city.country::string = 'US'
  and v:city.name in ('Dallas', 'Chicago', 'Boston')
)
FILE_FORMAT = ff_json_gz
OVERWRITE = TRUE;

-- step 4b: Export weather data as json files
-- 2 months, all US

copy into @ext_stg_export/weather/daily_16_total_2018_jan_feb_
from (select v
from sample_data.weather.daily_16_total
where t between to_timestamp('01/01/2017', 'MM/DD/YYYY') and
                dateadd('day', -1, dateadd('month', 1,     -- monthend
                to_timestamp('02/01/2017', 'MM/DD/YYYY')) )
  and v:city.country::string = 'US'
)
FILE_FORMAT = ff_json_gz
OVERWRITE = TRUE;


-- step 4d - create view to flatten weather data
--
create or replace view v_daily_16_total_elt as
select  d.v:city.id::number city_id,
        d.v:city.name::varchar(255) city,
        d.v:city.country::varchar(255) country,
        d.v:city.coord.lat::float lat,
        d.v:city.coord.lon::float lon,
        d.v:time::timestamp time,
        da.value:clouds::number clouds,
        da.value:deg::number deg,
        da.value:dt::timestamp dt,
        da.value:humidity::number humidity,
        da.value:pressure::float pressure,
        da.value:uvi::float uvi,
        da.value:speed::float speed,
        da.value:temp.day::float temp_day,
        da.value:temp.eve::float temp_eve,
        da.value:temp.max::float temp_max,
        da.value:temp.min::float temp_min,
        da.value:temp.morn::float temp_morn,
        da.value:temp.night::float temp_night,
        w.value:id::number weather_id,
        w.value:description::varchar(1000) weather_description,
        w.value:main::varchar(255) weather_main,
        w.value:icon::varchar(255) weather_icon,
       // d.v,
        d.t
  from  sample_data.weather.daily_16_total d,
        lateral flatten( input => d.v:data ) da,
        lateral flatten( input => da.value:weather ) w
  ;

-- step 4d: Export weather data as TSV files
-- one month, 4 cities

copy into @ext_stg_export/weather/test/TSV/daily_16_total_2018_jan_
from (select *
from V_DAILY_16_TOTAL_ELT
where t between to_timestamp('01/01/2017', 'MM/DD/YYYY') and
                dateadd('day', -1, dateadd('month', 1,     -- monthend
                to_timestamp('01/01/2017', 'MM/DD/YYYY')) )
  and country = 'US'
  and city in ('Dallas', 'Chicago', 'Boston')
)
FILE_FORMAT = ff_tsv_gz
HEADER = TRUE
OVERWRITE = TRUE;

-- step 4b: Export weather data as json files
-- 2 months, all US

copy into @ext_stg_export/weather/test/TSV/daily_16_total_2018_jan_feb_
from (select *
from V_DAILY_16_TOTAL_ELT
where t between to_timestamp('01/01/2017', 'MM/DD/YYYY') and
                dateadd('day', -1, dateadd('month', 1,     -- monthend
                to_timestamp('02/01/2017', 'MM/DD/YYYY')) )
  and country = 'US'
)
FILE_FORMAT = ff_tsv_gz
HEADER = TRUE
OVERWRITE = TRUE;