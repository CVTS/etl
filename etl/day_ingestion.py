import os
import sys
import logging
import shutil
from datetime import date
from multiprocessing import Pool
from os.path import join, isfile
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)


class processDayData(object):
    def __init__(self, work_fldr, output_folder, files_at_a_time, num_workers, dt_conn_string):
        self.fldr = work_fldr
        self.target_folder = output_folder
        self.files_at_a_time = files_at_a_time
        self.pool = None
        self.num_workers = num_workers

        # Holds the information on the correspondence between the vehicle IDs in the original data
        # And the integers we are using to identify the vehicles in our processing
        self.engine = create_engine(dt_conn_string, echo=True)
        self.conn = self.engine.connect()
        self.veh_idx = pd.DataFrame()
        self.veh_types = pd.DataFrame()
        self.dfs = {}
        self.df = []

    def process(self):
        day = self.fldr.split('/')[-1]
        tot = [x[0] for x in self.conn.execute('Select count(*) from days_ingested where day=?', [day])][0]
        if tot:
            print(f'Day {day} already processed. Skipping')
            return

        # It cleans the output folder in case it crashed in the middle
        shutil.rmtree(self.target_folder)
        os.makedirs(self.target_folder)

        self.veh_idx = pd.read_sql('select vehicle_id, vehicle_id_string vehicle from vehicle_ids', self.conn)
        self.veh_types = pd.read_sql(
            'select vehicle_id_string vehicle, vehicle_type VehicleType, 1 in_database from vehicle_types',
            self.conn)

        file_list = sorted([x for x in os.walk(self.fldr) if x[2]][0][2])
        parquet_sets = []
        curr_set = []
        for fl in file_list:
            curr_set.append(fl)
            if len(curr_set) >= self.files_at_a_time:
                parquet_sets.append(curr_set)
                curr_set = []
        if curr_set:
            parquet_sets.append(curr_set)
        for i, set_of_data in enumerate(parquet_sets):
            self.extract(set_of_data)
            self.transform()
            self.load()

        if parquet_sets:
            today = date.today().strftime("%d/%m/%Y")
            self.conn.execute('Insert into days_ingested(day, ingestion_date) VALUES(?,?)',
                              [self.fldr.split('/')[-1], today])

    def extract(self, set_of_data):
        print(f'Loading {len(set_of_data)} data files')
        self.df = [pd.read_parquet(join(self.fldr, fl)) for fl in set_of_data]
        self.df = pd.concat(self.df)
        print(f'Loaded {self.df.shape[0]:,} records')

    def transform(self):
        # Filters to update tables
        new_veh_types = self.df[['vehicle', 'VehicleType']].drop_duplicates()
        new_veh_id = new_veh_types[['vehicle']].drop_duplicates()

        # Updates types table in memory
        new_veh_types = new_veh_types.assign(in_database=0)
        new_veh_types = pd.concat([new_veh_types, self.veh_types])

        self.veh_types = new_veh_types.drop_duplicates(subset=['vehicle', 'VehicleType'])
        self.veh_types.loc[:, 'in_database'] = 1

        # Appends the vehicle type table in the database
        new_veh_types.drop_duplicates(subset=['vehicle', 'VehicleType'], keep=False, inplace=True)
        new_veh_types = new_veh_types[new_veh_types.in_database == 0]
        new_veh_types = new_veh_types[['vehicle', 'VehicleType']]
        new_veh_types.columns = ['vehicle_id_string', 'vehicle_type']
        new_veh_types.to_sql('vehicle_types', self.conn, if_exists='append', index=False)
        print(f'    New vehicle types - appended {new_veh_types.shape[0]}')

        # Appends the vehicle ID table
        new_ids = new_veh_id.loc[~new_veh_id.vehicle.isin(self.veh_idx.vehicle), 'vehicle'].values
        new_veh_id = new_veh_id.loc[~new_veh_id.vehicle.isin(self.veh_idx.vehicle), :]
        max_id = self.veh_idx.vehicle_id.max() if self.veh_idx.shape[0] else 0
        new_id_lookup = pd.DataFrame({'vehicle_id': np.arange(new_ids.shape[0]) + 1 + max_id, 'vehicle': new_ids})
        self.veh_idx = pd.concat([self.veh_idx, new_id_lookup])

        # Create the new reference table
        new_veh_id = new_veh_id.merge(self.veh_idx, on='vehicle', how='left')[['vehicle_id', 'vehicle']]
        new_veh_id.columns = ['vehicle_id', 'vehicle_id_string']
        new_veh_id.to_sql('vehicle_ids', self.conn, index=False, if_exists='append')
        print(f'    New vehicle IDs - appended {new_veh_id.shape[0]}')

        # Merges lookup table and cleans data types
        self.df = self.df.merge(self.veh_idx, on='vehicle')[['datetime', 'speed', 'x', 'y', 'heading', 'vehicle_id']]
        self.dfs = {veh_id: x for veh_id, x in self.df.groupby(self.df['vehicle_id'])}
        del self.df

    def load(self):
        pool = Pool(processes=self.num_workers, )
        for veh_id, df in self.dfs.items():
            pool.apply_async(write_down, [df, self.target_folder])
        pool.close()
        pool.join()
        self.dfs.clear()


def read_parquet_from_disk(file_name):
    return pd.read_parquet(file_name)


def write_down(df, target_folder):
    """Writes individual dataframes to disk"""
    table_name = f'vehicle_{df.vehicle_id.values[0]}'
    df.drop(columns=['vehicle_id'], inplace=True)
    fl = join(target_folder, f'{table_name}.gzip')
    df.to_csv(fl, mode='a', index=False, header=not isfile(fl))


if __name__ == '__main__':
    fldr = sys.argv[1]
    output_folder = sys.argv[2]
    files_at_a_time = sys.argv[3]
    num_workers = sys.argv[4]
    database_connection_string = sys.argv[5]

    # t = perf_counter()
    # fldr = 'D:/cvts/20200402/'
    # output_folder = 'D:/cvts'
    # files_at_a_time = 1
    # num_workers = 30
    # database_connection_string = 'sqlite:///D:/cvts/index.sqlite'

    mp_class = processDayData(fldr, output_folder, files_at_a_time, num_workers, database_connection_string)
    mp_class.process()
