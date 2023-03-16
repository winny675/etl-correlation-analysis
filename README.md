# final-presentation-etl-correlation-analysis

Create docker compose ( Airflow, Postgresql ) on your local computer.

Make a data architecture diagram that can describe how
how do you extract the raw data json & csv into the postgresql database.

Like the case example above, create a dataflow diagram that describes the data warehouse layer.

Draw / create an ER Diagram (Entity Relationship) that describes
relationship between tables that are ingested into mysql (ODS Layer)

From the raw data, create data modeling using Kimbal Star Schema. Decide which one
is a fact table, which one is a dim table and draw the diagram.

Make one dag in airflow with task :
a. Extract Load data from raw data to ods layer
b. Extract and load data from ods layer to dwh layer ( dim & fact )
c. Transform data from dwh layer to serving layer

In the serving layer create an aggregation table that does join fact and dim table yelp data and climate data

Make an analysis of the aggregation table in point 7, to find the correlation between reviews, tips and weather.

Make visual form
