# Sparkify Data Lake

## Purpose of database

In this theoretical case Sparkify decided to change its database design to a datalake using Spark,
as the amount of data created by its users that was saved in the original Redshift data warehouse could not be 
saved and processes at the speed needed to run the necessary analytical steps quick enough.

Therefore the data was loaded into a data lake using Spark using an ELT process, which means that the data is first loaded
and only transformed into star schema modeled analytics tables when needed (schema-on-read).

## Schema

The analytics tables centers on the `sonplays` table as fact table, which contains a log of user song plays.
It has foreign keys to the following dimension tables:

* `users`
* `songs`
* `artists`
* `time`

## Instructions

### Create `dl.cfg` file
You will need to create a configuration file `dl.cfg` with the following
structure:

```
[AWS]
AWS_ACCESS_KEY_ID=<your_aws_access_key_id>
AWS_SECRET_ACCESS_KEY=<your_aws_secret_access_key>

```

### Use default S3 bucket

You can use the default public s3 bucket (`s3a:/silviomartin-udac-9087/`) to save the final files or create your own bucket and insert its address under `output_data=`


### Execute etl.py

You can execute the ETL pipeline from the command line by entering `python etl.py`.

