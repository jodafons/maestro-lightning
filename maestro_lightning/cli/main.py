#!/usr/bin/env python3

import sys
import argparse

from maestro_lightning import get_argparser_formatter
from .parsers.job      import job_parser, run_job
from .parsers.task     import task_parser, run_init, run_next


def build_argparser():

    formatter_class = get_argparser_formatter()

    parser    = argparse.ArgumentParser(formatter_class=formatter_class)
    mode = parser.add_subparsers(dest='mode')


    run_parent = argparse.ArgumentParser(formatter_class=formatter_class, add_help=False, )
    option = run_parent.add_subparsers(dest='option')
    option.add_parser("job" , parents = job_parser()   ,help='Run job runner.',formatter_class=formatter_class)
    option.add_parser("task", parents = task_parser()  ,help='Run the task init',formatter_class=formatter_class)
    option.add_parser("next", parents = task_parser()  ,help='Run the task finalizing',formatter_class=formatter_class)
    mode.add_parser( "run", parents=[run_parent], help="",formatter_class=formatter_class)
    
    
    
    task_parent = argparse.ArgumentParser(formatter_class=formatter_class, add_help=False, )
    option = task_parent.add_subparsers(dest='option')
    #option.add_parser("create"   , parents = create_parser()    ,help='',formatter_class=formatter_class)
    #option.add_parser("retry"    , parents = retry_parser()    ,help='',formatter_class=formatter_class)
    #option.add_parser("status"   , parents = status_parser()    ,help='',formatter_class=formatter_class)
    #mode.add_parser( "task", parents=[task_parent], help="",formatter_class=formatter_class)

    return parser

def run_parser(args):
    if args.mode == "run":
        if args.option == "job":
            run_job(args)
        
      

def run():

    parser = build_argparser()
    if len(sys.argv)==1:
        print(parser.print_help())
        sys.exit(1)

    args = parser.parse_args()
    run_parser(args)



if __name__ == "__main__":
  run()