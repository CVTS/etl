Create TABLE vehicles (vehicle_id        INTEGER NOT NULL PRIMARY KEY,
                          vehicle_id_string TEXT    NOT NULL,
                          vehicle_type_id   SMALLINT);
CREATE UNIQUE INDEX unique_veh_id ON vehicle_ids (vehicle_id_string);


CREATE TABLE vehicle_types (id               SMALLINT NOT NULL,
                            type_vn          TEXT,
                            type_en          TEXT,
                            group_name       TEXT,
                            vehicle_type     TEXT);


Create TABLE vehicle_days  (vehicle_id  INTEGER   NOT NULL,
                            day         DATE      NOT NULL,
                            pings       INTEGER   NOT NULL,
                            min_time    TIMESTAMP NOT NULL,
                            max_time    TIMESTAMP NOT NULL,
                            geom        geometry(Polygon, 4326));
CREATE INDEX veh_days_brin ON vehicle_days USING BRIN (geom);
CREATE INDEX vehicle_days_id_idx ON vehicle_days (vehicle_id);
CREATE INDEX vehicle_days_day_idx ON vehicle_days (day);

Create TABLE days_ingested  (day            TEXT      NOT NULL,
                             ingestion_date TIMESTAMP NOT NULL);