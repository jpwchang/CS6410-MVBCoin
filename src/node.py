################################################################################
#
# node.py
#
# Main file for running the MVBCoin node
#
################################################################################

import argparse

from bootstrap_node import bootstrap_node
from miner_node import miner_node

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-bootstrap", action="store_true")
    parser.add_argument("-numworkers", action="store", type=int, default=1)
    parser.add_argument("-ports", action="store", nargs="+", type=int, default=[10000])
    parser.add_argument("-numtxinblock", action="store", type=int, default=50000)
    parser.add_argument("-difficulty", action="store", type=int, default=1)
    args = parser.parse_args()

    # For milestone 1: force single thread!
    num_workers = 1 # ignore args.numworkers
    ports = [args.ports[0]] # drop all but the first port

    # handle the bootstrap case
    if args.bootstrap:
        bootstrap_node()
    else:
        miner_node(num_workers, ports[0], args.numtxinblock, args.difficulty)

if __name__ == '__main__':
    main()
