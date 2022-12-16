-- creating roles and constructing a hierarchy.
create role ADMIN;

grant role ADMIN To role ACCOUNTADMIN;

create role DEVELOPER;

grant role DEVELOPER To role ADMIN;

create role PII;

grant role PII To role ACCOUNTADMIN;

-- creating M-sized warehouse
create
or replace warehouse assignment_wh;

-- using role admin
use role admin;

-- creating database
create
or replace database assignment_db;

-- creating schema
create
or replace schema my_schema;

-- creating table for loading data from internal stage
create
or replace table SAMPLE_DEPARTMENT_DATASET(
    ID VARCHAR(1000),
    DEPT_NAME VARCHAR(1000),
    LOCATION VARCHAR(1000),
    TRAVEL_REQUIRED VARCHAR(1000),
    elt_by VARCHAR(1000) DEFAULT 'LOCAL',
    elt_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(1000) DEFAULT 'Department_Dataset.csv'
);

-- creating table for loading data from external stage
create
or replace table SAMPLE_DEPARTMENT_DATASET_EXTERNAL(
    ID VARCHAR(1000),
    DEPT_NAME VARCHAR(1000),
    LOCATION VARCHAR(1000),
    TRAVEL_REQUIRED VARCHAR(1000),
    elt_by VARCHAR(1000) DEFAULT 'External',
    elt_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(1000) DEFAULT 'Department_Dataset.csv'
);

-- creating a variant dataset
create
or replace table dept_dataset_copy as
select
    *
from
    SAMPLE_DEPARTMENT_DATASET;

desc table dept_dataset_copy;

-- creating internal stage
create
or replace stage my_internal_stage;

put file: / / / Users / mansigupta / Downloads / Department_Dataset.csv @my_internal_stage;

-- creating external stage
create
or replace stage aws_my_stage;

-- uploading data into S3 bucket.
-- copying data from internal stage into table
copy into SAMPLE_DEPARTMENT_DATASET(ID, DEPT_NAME, LOCATION, TRAVEL_REQUIRED)
from
    @my_internal_stage file_format = my_format;

-- copying data from S3 bucket into table 
COPY INTO SAMPLE_DEPARTMENT_DATASET_EXTERNAL(ID, DEPT_NAME, LOCATION, TRAVEL_REQUIRED)
from
    's3://kartik-drive-glue-prac/data/Department_Dataset.csv' file_format =(type = CSV) credentials = (
        aws_key_id = 'AKIA5W7VA6U5FXEIA5AJ' aws_secret_key = '8NXQSpu3t19UrTb3ylsmtNFYZ5E1iqqMXAguUHhp'
    );

-- creating stage for parquet files
create
or replace stage my_stage;

put file: / / / Users / mansigupta / Downloads / cities.parquet @my_stage;

create file format my_parquet_format type = parquet;

-- inferschema for parquet files
select
    *
from
    table(
        infer_schema(
            location = > '@my_stage',
            file_format = > 'my_parquet_format'
        )
    );

-- Masking the coloumn
use role ACCOUNTADMIN;

grant create masking policy on schema ASSIGNMENT_DB.MY_SCHEMA to role PII;

grant apply masking policy on account to role PII;

grant role PII to user Mansi;

create
or replace masking policy to_location_mask as (val string) returns string -> case
    when current_role() in ('PII') then val
    else '**masked**'
end;

alter table
    if exists SAMPLE_DEPARTMENT_DATASET
modify
    column location
set
    masking policy to_location_mask;

-- checking for mask on location column
use secondary roles all;

use role PII;

select
    *
from
    SAMPLE_DEPARTMENT_DATASET;

use role developer;

select
    *
from
    SAMPLE_DEPARTMENT_DATASET;