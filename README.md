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
2. Build the Pipeline container: **docker compose build pipeline**
3. Execute the Docker Compose:
    - Postgres Database in Detached mode: **docker compose up -d db**
    - Python Pipeline: **docker compose run pipeline**

4. The pipeline will run the base code:
    1. to pull API result for 10 locations, first finding the Latitude and Longitude of OpenWeatherMap Geocoder API
    2. For each resulted Lat/Lon will then call OpenWeatherMap TimeMachine API of last 5 days of today's in (UTC)
    3. It will parse the result adding the Location found in Geocoder (City Name, State Name, Country Code, Latitude, Longitude, Date in Text format)
    4. To save the result in Database, code will first check in Postgres Database if table **(public.weather)** exists:
        1. If table **(public.weather)** does exist, it will open a Database Transaction:
            - First to Delete the Locations and Distinct Dates existing in the Dataframe
            - Save the Dataframe result into the table **(public.weather)**
        2. If table **(public.weather)** does not exist, it just save the Dataframe data into the target table (creating the table in database)
    5. For each analytical datasets required:
        1. Whitin a Database Transaction
            - It drops the table if exists in database if it exists
            - It creates a table using a select statement for each business logic
            - The tables to be created are: 
                - **public.weather_date_stats**
                - **public.highest_temp**

Evidences:

1. When first run the Pipeline, and table public.weather does not exist:

![image](https://user-images.githubusercontent.com/39410838/184519317-e80ad9ba-704a-4aa0-87df-38e2a67d3b4b.png)

2. Tables gets created in Postgres database:

![image](https://user-images.githubusercontent.com/39410838/184519324-d8560b85-9e9f-43eb-9769-fc0d44d916dd.png)

3. Table public.weather is populated:

![image](https://user-images.githubusercontent.com/39410838/184519349-c0a088c5-6058-47dc-ab1b-5f0ecb554bbb.png)

4. Table public.highest_temp is also created, showing the date and highest temperature for each city:

![image](https://user-images.githubusercontent.com/39410838/184519360-28279253-5a77-465e-92dc-55e65ad1a4e0.png)

5. Table public.weather_date_stats is also created, showing the last 5 days, average/min/max temperature and the city where the max temperature reached:

![image](https://user-images.githubusercontent.com/39410838/184519374-d75d562d-f9b7-4817-9ab7-d78b22e422d4.png)

6. When subsequent runs, as tables exists, it gets first existing records (latitude, longitude and dt column) and then inserting fresh data to the table (public.weather) and recalculating analytical tables (public.highest_temp and public.weather_date_stats):

![image](https://user-images.githubusercontent.com/39410838/184519432-98445a93-c9bc-401e-aea6-27999259a4e3.png)




