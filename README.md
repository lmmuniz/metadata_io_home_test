# Metadata.IO Home Test
## Candidate: Leonardo Mairene Muniz
## E-mail: lmmuniz@gmail.com

### Home Test Instructions provided:
### ---------------------------------
Data Engineer take-home test

Create Workflow in Python according to the following requirements:

Extract the last 5 days of data from the free API: https://api.openweathermap.org/data/2.5/onecall/timemachine (Historical weather data) from 10 different locations to choose by the candidate.

Build a repository of data where we will keep the data extracted from the API. This repository should only have deduplicated data. Idempotency should also be guaranteed.

Build another repository of data that will contain the results of the following calculations from the data stored in step 2.

A dataset containing the location, date and temperature of the highest temperatures reported by location and month.
A dataset containing the average temperature, min temperature, location of min temperature, and location of max temperature per day.


Extra information:

The candidate can choose which kind of db, or data formats are used as a repository of data for steps 2 and 3.
The deliverable should contain a docker-compose file so it can be run by running ‘docker-compose up’ command. If the workflow relies on any database or any other middleware, this docker-compose file should have all what is necessary to make the workflow work (except passwords for the API or any other secret information)
The code should be well structured and add necessary log traces to easily detect problems. 


Please give access to your repository to :
francisco.martin@metadata.io
emily.hoang@metadata.io
administrant@optimhire.com

Thank you and good luck !!
Metadata Engineering

### ---------------------------------

The result pipeline is using a Docker Compose solution with:

1. a Docker Compose Service (db) to represent a Postgres Database exposing port 5432 in host 'db' for database name 'metadata' and user/pwd 'metadata'

2. a Docker Compose Service (pipeline) a Python Linux image to run the code to extract OpenWeatherMap API

In order to build and run, follow the below steps:

1. Make sure in docker-compose.yml, the Environment Variable API_KEY is using a valid OpenWeatherMap access key
2. Build the Pipeline container: docker compose build pipeline
3. Execute the Docker Compose:
    3.1. Postgres Database in Detached mode: docker compose up -d db
    3.2. Python Pipeline: docker compose run pipeline

4. The pipeline will run the base code:
    1. to pull API result for 10 locations, first finding the Latitude and Longitude of OpenWeatherMap Geocoder API
    2. For each resulted Lat/Lon will then call OpenWeatherMap TimeMachine API of last 5 days of today's in (UTC)
    3. It will parse the result adding the Location found in Geocoder (City Name, State Name, Country Code, Latitude, Longitude, Date in Text format)
    4. To save the result in Database, code will first check in Postgres Database if table (public.weather) exists:
        1. If table (public.weather) does exist, it will open a Database Transaction:
            - First to Delete the Locations and Distinct Dates existing in the Dataframe
            - Save the Dataframe result into the table (public.weather)
        2. If table (public.weather) does not exist, it just save the Dataframe data into the target table (creating the table in database)
    5. For each analytical datasets required:
        1. Whitin a Database Transaction
            - It drops the table if exists in database if it exists
            - It creates a table using a select statement for each business logic
            - The tables to be created are: 
                - public.weather_date_stats
                - public.highest_temp


