#!/usr/bin/env python

from git_fast_filter import Blob, Reset, FileChanges, Commit, FastExportFilter
import logging

def parse_rename(rename_str):
    if "=" not in rename_str:
        raise ValueError("Rename arguments should be of the form src=target; %s is not valid" % rename_str)
    return tuple(rename_str.split("=", 1))

def main(args):
    branch_renames = dict(args.branch_renames)
    branch_pattern_renames = args.branch_pattern_renames
    branch_excludes = dict(args.branch_excludes)
    file_renames = dict([(src, target) for src, target in args.file_renames if not src.endswith("/")])
    dir_renames = [(src, target) for src, target in args.file_renames if src.endswith("/")]
    file_excludes = set([target for target in args.file_excludes if not target.endswith("/")])
    dir_excludes = [target for target in args.file_excludes if target.endswith("/")]
    branches_found = set()
    files_found = set()
    def my_commit_callback(commit):
        branch = commit.branch
        for src_pattern, target in branch_pattern_renames:
            branch = re.sub(src_pattern, target, branch)
        if branch in branch_renames:
            branch = branch_renames[branch]
        elif branch in branch_excludes:
            if branch not in branches_found:
                logging.info("Excluding branch %s", branch)
                branches_found.add(branch)
            commit.skip()
            return
        if branch != commit.branch:
            if commit.branch not in branches_found:
                logging.info("Renaming branch %s to %s", commit.branch, branch)
                branches_found.add(commit.branch)
            commit.branch = branch
        new_file_changes, alter_commit = [], False
        for change in commit.file_changes:
            exclude_file = False
            filename = change.filename
            for src_dir, target_dir in dir_renames:
                if filename.startswith(src_dir):
                    filename = filename.replace(src_dir, target_dir, 1)
            for target_dir in dir_excludes:
                if filename.startswith(target_dir):
                    exclude_file = True
            if filename in file_renames:
                filename = file_renames[filename]
            if filename in file_excludes:
                exclude_file = True
            if exclude_file:
                if filename not in files_found:
                    logging.info("Excluding file %s", filename)
                    files_found.add(filename)
                alter_commit = True
            else:
                new_file_changes.append(change)
            if filename != change.filename:
                if change.filename not in files_found:
                    logging.info("Renaming file %s to %s", change.filename, filename)
                    files_found.add(change.filename)
                change.filename = filename
        if alter_commit:
            if new_file_changes:
                commit.file_changes = new_file_changes
            else:
                commit.skip()
    filter = FastExportFilter(commit_callback = my_commit_callback)
    filter.run()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    import argparse
    parser = argparse.ArgumentParser(description="git export filter for renaming files and branches")
    parser.add_argument("-b", "--branch", action="append", default=[], dest="branch_renames", type=parse_rename,
                        help="src_branch=target_branch: renames branches (must match full branch e.g. refs/thebranch)")
    parser.add_argument("-B", "--branch-pattern", action="append", default=[], dest="branch_pattern_renames", type=parse_rename,
                        help="src_pattern=target: renames branches (does regex-based pattern substitution)")
    parser.add_argument("-f", "--file", action="append", default=[], dest="file_renames", type=parse_rename,
                        help="src=target: renames file (whole path); src/=target/: renames all files in directory")
    parser.add_argument("-X", "--exclude-branch", action="append", default=[], dest="branch_excludes",
                        help="excludes branches from filter (must match full branch e.g. refs/thebranch)")
    parser.add_argument("-x", "--exclude", action="append", default=[], dest="file_excludes",
                        help="target: excludes file (whole path); target/: excludes all files in directory")
    args = parser.parse_args()
    main(args)
