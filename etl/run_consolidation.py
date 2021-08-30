#!/usr/bin/env python3

from consolidate_month import ConsolidateWholeMonth


def main():
    num_workers = 32
    for month in range(6, 7):
        cwm = ConsolidateWholeMonth(month, num_workers)
        cwm.consolidate()
        # scp -P 2235 run_consolidation.py consolidate_month.py ../requirements.txt ubuntu@103.160.90.151:/mnt/data_lake/src


if __name__ == '__main__':
    main()
