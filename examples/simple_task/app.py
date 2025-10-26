#!/usr/bin/env python
import os
import sys
import json
import argparse
import itertools
import numpy as np

from time   import sleep
from pprint import pprint
from loguru import logger



def run():

    parser = argparse.ArgumentParser(description = '', add_help = False)
    parser = argparse.ArgumentParser()

    parser.add_argument('-j','--job', action='store', dest='job', required = True,
                        help = "The job input")
    parser.add_argument('-o','--output', action='store', dest='output', required = False, default='circuit.json',
                        help = "The job output")

    if len(sys.argv)==1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()

    basepath = os.getcwd()

    with open( args.job , 'r') as f:
        d = json.load(f)
        a = d['a']
        b = d['b']
        pprint(d)
        
    sleep(a)
    sleep(b)
    o = {'a':a, 'b':b, 'c':a+b}

    logger.info(f"saving into {args.output}...")
    with open(args.output , 'w') as f:
        json.dump(o, f)

    sys.exit(0)

if __name__ == "__main__":
    run()