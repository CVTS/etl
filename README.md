# Extract Transform and Load (ETL)
Data ingestion pipelines and structure for CVTS data

## Overview
Data is sourced from Hanels servers in CSV format which is not very easy to use. 

This process transforms this data into compressed parquet files which are stored in a floating disk on the FPT cloud called "DataLake". 

The metadata which ties these files together is stored in a PG database on the PostGis instance called "datalake".

### Files on disk
Files on disk are stored as

```
/mnt
  |- datalake
    |- 2020
      |- 04
        |- vehicle_<veh_id>.gz
```

These files can be loaded easily using pandas

```python
filename = f'/mnt/data_lake/2020/04/vehicle_{veh_1.vehicle_id}.zip'
df = pd.read_csv(filename)
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['x'], df['y']))
gdf.plot()
```

### Using the database

To find out what files are available for loading you can read from the database.

```python
with pg.connect("postgresql://cvts@10.100.0.50:5432/datalake") as conn:
    vehicles = pd.read_sql("select * from vehicles;", conn)
vehicles.head()
```