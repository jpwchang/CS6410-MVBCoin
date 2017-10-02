################################################################################
#
# node.py
#
# Main file for running the MVBCoin node
#
################################################################################

import argparse
import sys

from bootstrap_node import bootstrap_node
from miner_node import miner_node

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-bootstrap", "--bootstrap", action="store_true")
    parser.add_argument("-numworkers", "--numworkers", action="store", type=int, default=1)
    parser.add_argument("-numcores", "--numcores", action="store", type=int, default=1)
    parser.add_argument("-ports", "--ports", action="store", nargs="+", type=int, default=[10000])
    parser.add_argument("-numtxinblock", "--numtxinblock", action="store", type=int, default=50000)
    parser.add_argument("-difficulty", "--difficulty", action="store", type=int, default=1)
    parser.add_argument("-verbose", "--verbose", action="store_true")
    args = parser.parse_args()

    # check that the number of workers matches the number of ports
    if len(args.ports) != args.numworkers:
        print("numworkers must match number of ports!")
        sys.exit(0)

    # handle the bootstrap case
    if args.bootstrap:
        bootstrap_node()
    else:
        miner_node(args.numworkers, args.ports, args.numtxinblock, args.difficulty, args.numcores, args.verbose)

if __name__ == '__main__':
    main()
