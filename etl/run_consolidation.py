#!/usr/bin/env python3

from consolidate_month import ConsolidateWholeMonth


def main():
    num_workers = 32
    for month in range(6, 7):
        cwm = ConsolidateWholeMonth(month, num_workers)
        cwm.consolidate()

if __name__ == '__main__':
    main()
