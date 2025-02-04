# nuuka_energy_analytics 🚀

## description

### This repository contains the code and documentation for a simple ETL (Extract, Transform, Load) pipeline. The pipeline is designed to extract energy consumption data from the Helsinki OpenAPI Nuuka, load it into a PostgreSQL database, and perform transformations using PySpark. The repository is structured to follow best practices for version control and documentation.

### docker compoase
``` docker pull postgres:latest```

### create container with postgres database
```docker run --name analytics_db -e POSTGRES_PASSWORD=analytics@123 -d -p 15432:5432 postgres```

### Make sure validate the database connection in db client

### run the python script to retrive data from propertieslist and respective consume data using APIs

``` LIST_URL= https://helsinki-openapi.nuuka.cloud/api/v1.0/Property/List
ENERGY_DATA_URL = https://helsinki-openapi.nuuka.cloud/api/v1.0/EnergyData/Daily/ListByProperty ```

### run the ingestion
```  ingest_energy.py
```
### run incremntal to ingest data to the raw table

```  merge_energy_consumption.py
```
### create parquet file with snapshot data to find latest

``` energy_consume_snapshot.py


+--------------------+--------------+----------------+--------------------+
|            location|reportinggroup|latest_7_day_avg|       snapshot_date|
+--------------------+--------------+----------------+--------------------+
|4328 HYKS Psykiat...|   Electricity|      15516.3167|2025-02-04 21:31:...|
|4329 KivelΣn vanh...|   Electricity|      15516.3167|2025-02-04 21:31:...|
|      2247 Tukkutori|   Electricity|       14702.464|2025-02-04 21:31:...|
|4192 Laakson sair...|   Electricity|      12334.1081|2025-02-04 21:31:...|
|4370 Malmin pΣivy...|   Electricity|      12059.3806|2025-02-04 21:31:...|
+--------------------+--------------+----------------+--------------------+
```

### create branch
``` git checkout -b feature/incremental_merge ```
### Enhance/add new scripts, check the modifed/inserted files list
``` git status ```
### add files, commit and push
``` git add .
    git commit -m "Added the incremental load scripts" 
    git push```


