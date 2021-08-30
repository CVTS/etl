DROP TABLE IF EXISTS vehicle_ids;
DROP TABLE IF EXISTS unique_veh_id;
DROP TABLE IF EXISTS vehicle_types;
DROP TABLE IF EXISTS days_ingested;


Create TABLE IF NOT EXISTS vehicle_ids (vehicle_id        INTEGER NOT NULL PRIMARY KEY,
                                        vehicle_id_string TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS unique_veh_id ON vehicle_ids (vehicle_id_string);


CREATE TABLE IF NOT EXISTS vehicle_types (vehicle_id_string TEXT NOT NULL,
                                          vehicle_type      TEXT);


Create TABLE IF NOT EXISTS vehicle_days  (vehicle_id  INTEGER NOT NULL,
                                          day         TEXT NOT NULL,
                                          pings       INTEGER NOT NULL,
                                          min_y       REAL NOT NULL,
                                          max_y       REAL NOT NULL,
                                          min_x       REAL NOT NULL,
                                          max_x       REAL NOT NULL,
                                          min_time    INTEGER NOT NULL,
                                          max_time    INTEGER NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS unique_veh_id ON vehicle_ids (vehicle_id);


Create TABLE IF NOT EXISTS days_ingested  (day            TEXT NOT NULL,
                                           ingestion_date TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS unique_veh_id ON vehicle_ids (day);
