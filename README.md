```bash
py -3.12 -m venv venv && \
venv\Scripts\activate && \
pip install --upgrade pip && \
pip install -r requirements.txt
```

```bash
docker-compose up -d
```

## Notes

SQL schemas
```sql
CREATE SCHEMA  ods;
CREATE SCHEMA  dm;
CREATE SCHEMA  stg;
```

DDL `ods.fct_covid`:
```sql
CREATE TABLE  ods.fct_weather
(
    date varchar,
    iso_code varchar,
    continent varchar,
    location varchar,
    total_cases varchar, 
    new_cases varchar,    
    total_deaths varchar, 
    new_deaths varchar,   
    new_vaccinations varchar, 
    population varchar   
);
```

DDL `dm.fct_count_day_covid`:

```sql
CREATE TABLE IF NOT EXISTS dm.anomaly_day_weather (
    date varchar,
    curr_temp double precision,
    curr_precip double precision,
    is_temp_anomaly boolean,
    is_precip_anomaly boolean
);
```

DDL `dm.fct_avg_day_covid`:

```sql
CREATE TABLE IF NOT EXISTS dm.weather_trend_day (
    date varchar,
    curr_temp double precision,
    curr_precip double precision,
    temp_trend varchar,
    precip_trend varchar
);
```
