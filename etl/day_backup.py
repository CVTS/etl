import sys
from multiprocessing import Pool
import os
from os.path import join, isdir, basename
import pandas as pd
from time import perf_counter
import glob


class backupDayData(object):
    def __init__(self, work_fldr, output_folder, num_workers):
        self.fldr = work_fldr
        self.pool = None
        self.num_workers = num_workers
        self.target_folder = output_folder
        if not isdir(self.target_folder):
            os.mkdir(self.target_folder)

    def backup(self):
        files = list(glob.glob(f'{self.fldr}/*.csv'))
        new_files = [join(self.target_folder, basename(x)) for x in files]

        pool = Pool(processes=self.num_workers, )
        for source, target in zip(files, new_files):
            pool.apply_async(write_down, [source, target])
            # write_down(source, target)
        pool.close()
        pool.join()

def write_down(file_source, target_file):
    """Writes individual dataframes to disk"""
    t = perf_counter()
    target_file = target_file.replace('.csv', '.parquet')
    # print(f"{file_source}  ->  {target_file}")
    df = pd.read_csv(file_source)
    df.to_parquet(target_file, compression='brotli', index=False)
    print(f"{file_source}  ->  {target_file} [DONE IN {round(perf_counter() - t, 1)} s]")

if __name__ == '__main__':
    fldr = sys.argv[1]
    output_folder = sys.argv[2]
    num_workers = sys.argv[3]
    #
    # t = perf_counter()
    # fldr = 'D:/cvts/20200402/'
    # output_folder = 'D:/cvts/20200402/bkp'
    # num_workers = 30

    print(fldr, output_folder, num_workers)
    # mp_class = backupDayData(fldr, output_folder, num_workers)
    # mp_class.backup()
