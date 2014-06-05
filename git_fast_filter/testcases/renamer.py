#!/usr/bin/env python

from git_fast_filter import Blob, Reset, FileChanges, Commit, FastExportFilter
import logging

def parse_rename(rename_str):
    if "=" not in rename_str:
        raise ValueError("Rename arguments should be of the form src=target; %s is not valid" % rename_str)
    return tuple(rename_str.split("=", 1))

def main(args):
    branch_renames = dict(args.branch_renames)
    file_renames = dict([(src, target) for src, target in args.file_renames if not src.endswith("/")])
    dir_renames = [(src, target) for src, target in args.file_renames if src.endswith("/")]
    branches_found = set()
    files_found = set()
    def my_commit_callback(commit):
        branch = commit.branch
        if branch in branch_renames:
            if branch not in branches_found:
                logging.info("Found branch %s (renaming to %s)", branch, branch_renames[branch])
                branches_found.add(branch)
            commit.branch = branch_renames[branch]
        for change in commit.file_changes:
            filename = change.filename
            for src_dir, target_dir in dir_renames:
                if filename.startswith(src_dir):
                    filename = filename.replace(src_dir, target_dir, 1)
            if filename in file_renames:
                filename = file_renames[filename]
            if filename != change.filename:
                if change.filename not in files_found:
                    logging.info("Found file %s (renaming to %s)", change.filename, filename)
                    files_found.add(change.filename)
                change.filename = filename
    filter = FastExportFilter(commit_callback = my_commit_callback)
    filter.run()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    import argparse
    parser = argparse.ArgumentParser(description="git export filter for renaming files and branches")
    parser.add_argument("-b", "--branch", action="append", default=[], dest="branch_renames", type=parse_rename,
                        help="src_branch=target_branch: renames branches (must match full branch e.g. refs/thebranch)")
    parser.add_argument("-f", "--file", action="append", default=[], dest="file_renames", type=parse_rename,
                        help="src=target: renames file (whole path); src/=target/: renames all files in directory")
    args = parser.parse_args()
    main(args)