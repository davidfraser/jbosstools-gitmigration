#!/usr/bin/env python

from git_fast_filter import Blob, Reset, FileChanges, Commit, FastExportFilter

def parse_rename(rename_str):
    if "=" not in rename_str:
        raise ValueError("Rename arguments should be of the form src=target; %s is not valid" % rename_str)
    return tuple(rename_str.split("=", 1))

def main(args):
    branch_renames = dict(args.branch_renames)
    def my_commit_callback(commit):
        if commit.branch in branch_renames:
            commit.branch = branch_renames[commit.branch]
    filter = FastExportFilter(commit_callback = my_commit_callback)
    filter.run()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="git export filter for renaming files and branches")
    parser.add_argument("-b", "--branch", action="append", default=[], dest="branch_renames", type=parse_rename)
    args = parser.parse_args()
    main(args)