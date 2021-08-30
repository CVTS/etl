import os
import logging
from os.path import join, isdir
from time import perf_counter
from datetime import datetime
from sqlalchemy import create_engine
from day_ingestion import processDayData

logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)


def ingest_month():
    files_at_a_time = 20
    num_workers = 31
    dt_conn_string = 'sqlite:////mnt/data_lake/2020/index.sqlite'
    engine = create_engine(dt_conn_string, echo=True)
    conn = engine.connect()

    conn.execute('''Create table if not exists vehicle_ids (vehicle_id        INTEGER NOT NULL PRIMARY KEY,
                                                            vehicle_id_string TEXT NOT NULL)''')

    conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS unique_veh_id ON vehicle_ids (vehicle_id_string)')

    conn.execute('''Create table if not exists vehicle_types (vehicle_id_string TEXT NOT NULL,
                                                              vehicle_type      TEXT)''')


    conn.execute('''Create table if not exists vehicle_days  (vehicle_id  INTEGER NOT NULL,
                                                              day         TEXT NOT NULL,
                                                              pings       INTEGER NOT NULL,
                                                              min_y       REAL NOT NULL,
                                                              max_y       REAL NOT NULL,
                                                              min_x       REAL NOT NULL,
                                                              max_x       REAL NOT NULL,
                                                              min_time    INTEGER NOT NULL,
                                                              max_time    INTEGER NOT NULL)''')

    conn.execute('''Create table if not exists days_ingested  (day            TEXT NOT NULL,
                                                               ingestion_date TEXT NOT NULL)''')


    folder_pairs = [['/mnt/backup_data/compressed_parquet/202004', '/mnt/data_lake/2020/04'],
                    ['/mnt/backup_data/compressed_parquet/202005', '/mnt/data_lake/2020/05'],
                    ['/mnt/backup_data/compressed_parquet/202006', '/mnt/data_lake/2020/06']]

    for work_fldr, output_folder in folder_pairs:
        days = sorted([x[1] for x in os.walk(work_fldr) if x[1]][0])
        for day in days:
            print('Starting day', datetime.now().strftime("%H:%M:%S"))
            t = perf_counter()
            data_folder = join(work_fldr, day)
            target_folder = join(output_folder, day)
            if not isdir(target_folder):
                os.mkdir(target_folder)
            print(data_folder, target_folder)
            process = processDayData(data_folder, target_folder, files_at_a_time, num_workers, dt_conn_string)
            process.process()
            print('     Took: ', round((perf_counter() - t) / 60, 1), 'minutes')

    # scp  ubuntu@103.160.90.151:/mnt/data_lake/2020/index.sqlite D;/
    # scp ingest_month.py day_ingestion.py ../requirements.txt ubuntu@103.160.90.151:/mnt/data_lake/src
    #### scp compress_and_backup_month.py day_backup.py ../requirements.txt -i ~/.ssh/wb_mongo wb@103.160.90.139:/var/lib/mongo/etl



if __name__ == '__main__':
    ingest_month()
