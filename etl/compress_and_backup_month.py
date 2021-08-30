#!/usr/bin/env python3

import os
from day_backup import backupDayData
from pathlib import Path

def main():

    from_folder = '/mnt/postgis-data/csv_data/may_2020'
    to_folder = '/mnt/backup_data/compressed_parquet/202005'
    num_workers = 12

    for x in range(29,30):
        input_day = f"{from_folder}/202005{x:02}"
        output_day = f"{to_folder}/202005{x:02}"
        print(f'Processing day: {input_day}')
        Path(output_day).mkdir(exist_ok=True, parents=True)
        mp_class = backupDayData(input_day, output_day, num_workers)
        mp_class.backup()

if __name__ == '__main__':
    main()
