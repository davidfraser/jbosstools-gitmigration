#!/usr/bin/env python

from git_fast_filter import Blob, Reset, FileChanges, Commit, FastExportFilter

def my_commit_callback(commit):
  if commit.branch == "refs/heads/master":
    commit.branch = "refs/heads/slave"

def main(args):
    filter = FastExportFilter(commit_callback = my_commit_callback)
    filter.run()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="git export filter for renaming files and branches")
    args = parser.parse_args()
    main(args)