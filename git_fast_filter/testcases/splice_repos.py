#!/usr/bin/env python

import argparse
import datetime
import json
import os
import shutil
import sys
import logging
import tempfile
from os.path import abspath, basename, dirname, exists, join

from git_fast_filter import Reset, Commit, FastExportFilter, record_id_rename
from git_fast_filter import fast_export_output, fast_import_input
from git_fast_filter import _IDS
from git_fast_filter import FixedTimeZone

# TODO: allow incremental update
# TODO: make this a command-line option
KEEP_ON_ERROR = True

def datetime_to_json(d):
    z = None if (d.tzinfo is None) else d.tzinfo.tzname(d)
    return [d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond, z]

def json_to_datetime(d):
    year, month, day, hour, minute, second, microsecond, z = d
    tzinfo = None if z is None else FixedTimeZone(z)
    return datetime.datetime(year, month, day, hour, minute, second, microsecond, tzinfo)

class IntermediateFile(object):
    """Helpful wrapper object for intermediate files.
    Can handle random file names or fixed ones, in which case a previously created file will be reused
    Handles reopening the files for reading later, and tracking if the file is still in progress being created
    Handles failure and remembers whether the file has been closed"""
    def __init__(self, label, suffix, description, dir=None, fixed_name=None):
        self.label = label
        self.description = description
        self.exists = False
        self.in_progress = True
        self.closed = False
        if fixed_name:
            self.filename = os.path.join(dir or tempfile.gettempdir(), "%s-%s%s" % (label, fixed_name, suffix))
            if exists(self.filename) and os.stat(self.filename).st_size > 0:
                self.file = open(self.filename, 'rb')
                self.exists = True
                self.in_progress = False
            else:
                self.file = open(self.filename, 'wb')
        else:
            fd, self.filename = tempfile.mkstemp(prefix="%s-" % label, suffix=suffix, dir=dir)
            self.file = os.fdopen(fd, 'wb')

    def close(self, success=True):
        self.file.close()
        self.closed = True
        if success:
            self.in_progress = False

    def open_for_read(self):
        self.file = open(self.filename, "rb")
        self.closed = False

    def handle_failure(self, target_directory):
        status = "in-progress " if self.in_progress else ""
        if not self.closed:
            self.close(False)
        if not KEEP_ON_ERROR:
            try:
                os.remove(self.filename)
            except Exception, e:
                logging.warning("Error removing temporary file %s", self.filename)
        else:
            filename = join(target_directory, basename(self.filename))
            logging.warning("Storing %s%s for %s in %s", status, self.description, self.label, filename)
            if filename != self.filename:
                shutil.move(self.filename, filename)

    def remove_file(self):
        if not self.closed:
            self.close(True)
        try:
            os.remove(self.filename)
        except Exception, e:
            logging.warning("Error removing temporary file %s", self.filename)

class InterleaveRepositories:
    def __init__(self, args):
        self.input_repos = args.repos
        self.output_dir = args.output_repo
        self.failure_base_dir = dirname(abspath(self.output_dir))
        self.tmpdir = abspath(args.tmpdir)
        self.tmplabel = args.tmplabel
        self.intermediate_files = []

        # commit_branches maps branch -> repo -> ordered list of (commit_date, repo, commit_id)
        self.commit_branches = {}
        # just remember the commit dates for everything
        self.commit_dates = {}
        # commit_parents maps (repo, commit_id) -> (repo, parent_id)
        self.commit_parents = {}
        # combined_branches maps branch -> ordered list of (repo, commit_id)
        self.combined_branches = {}
        # changed_parents maps (repo, commit_id) -> (new_parent_repo, new_parent_id)
        self.changed_parents = {}
        # keeps track of which commit_ids have been written, so we don't write something before its dependencies
        self.written_commits = set()
        # maps branch -> (repo, commit_id) -> commit_object for those commits that have yet to be written
        self.pending_commits = {}

    def open_export_file(self, label, suffix, description):
        file_wrapper = IntermediateFile(label, suffix, description, dir=self.tmpdir, fixed_name=self.tmplabel)
        self.intermediate_files.append(file_wrapper)
        return file_wrapper

    def skip_reset(self, reset):
        reset.skip()

    @staticmethod
    def jsonize_commit(repo, commit):
        return (repo, commit.id, commit.branch, datetime_to_json(commit.committer_date), commit.from_commit)

    def remember_commits(self, stored_commits):
        for repo, commit_id, commit_branch, committer_date_parts, from_commit in stored_commits:
            committer_date = json_to_datetime(committer_date_parts)
            self.commit_branches.setdefault(commit_branch, {}).setdefault(repo, []).append(
                (committer_date, repo, commit_id))
            self.commit_parents[repo, commit_id] = (repo, from_commit)
            self.commit_dates[repo, commit_id] = committer_date


    def collect_commits(self):
        self.exported_stores = []
        for n, input_repo in enumerate(self.input_repos, start=1):
            exported_store = self.open_export_file(basename(input_repo), ".git-fast-export", "export")
            self.exported_stores.append(exported_store)
            remember_commits_file = self.open_export_file(basename(input_repo), ".remember-commits.json", "commit info")
            if exported_store.exists:
                if not remember_commits_file.exists:
                    raise ValueError("Cannot reuse previous export %s as %s has been lost",
                                     exported_store.filename, remember_commits_file.filename)
                stored_commits = json.load(remember_commits_file.file)
                remember_commits_file.close()
            else:
                stored_commits = []
                export = fast_export_output(input_repo)
                try:
                    collect = FastExportFilter(commit_callback=lambda c: stored_commits.append(self.jsonize_commit(n, c)))
                    collect.run(export.stdout, exported_store.file)
                    exported_store.close()
                    json.dump(stored_commits, remember_commits_file.file, indent=4)
                    remember_commits_file.close()
                except Exception, e:
                    logging.error("Error in memorizing export for %s [%d]: %s", input_repo, n, e)
                    raise
            self.remember_commits(stored_commits)

    def merge_branches(self):
        """for each branch, calculate how combining repos should change parents of commits"""
        branch_parents = {}
        for branch, repo_maps in self.commit_branches.items():
            self.combined_branches[branch] = combined_commits = []
            branch_maps = [repo_maps[repo] for repo in sorted(repo_maps)]
            last_repo, last_commit_id = None, None
            head_commits = [branch_map[0] for branch_map in branch_maps]
            np = (None, None)
            branch_parents[branch] = [self.commit_parents.get((repo, commit_id), np) for _, repo, commit_id in head_commits]
            while branch_maps:
                last_commits = [branch_map[-1] for branch_map in branch_maps]
                committer_date, repo, commit_id = max(last_commits)
                branch_map = repo_maps[repo]
                branch_map.pop(-1)
                combined_commits.append((repo, commit_id))
                if last_commit_id is not None and self.commit_parents[last_repo, last_commit_id] != (repo, commit_id):
                    self.changed_parents[last_repo, last_commit_id] = repo, commit_id
                last_repo, last_commit_id = repo, commit_id
                if not branch_map:
                    repo_maps.pop(repo)
                    branch_maps = [repo_maps[repo] for repo in sorted(repo_maps)]
            combined_commits.reverse()
        for branch, parent_commits in branch_parents.items():
            if (None, None) in parent_commits:
                # if one of the starts of the branches does not have any parents, skip it
                continue
            tail_repo, tail_commit_id = self.combined_branches[branch][0]
            parent_commits = [(self.commit_dates[repo, commit_id], repo, commit_id)
                              for repo, commit_id in parent_commits if commit_id is not None]
            parent_commits.sort()
            if not parent_commits:
                logging.info("Branch %s has no parents", branch)
                continue
            # TODO: Check that these parents all splice together nicely anyway
            _, branch_parent_repo, branch_parent_id = parent_commits[-1]
            logging.info("Found parent for branch %s tail %s:%s - %s:%s",
                         branch, tail_repo, tail_commit_id, branch_parent_repo, branch_parent_id)
            self.changed_parents[tail_repo, tail_commit_id] = branch_parent_repo, branch_parent_id

    def weave_commit(self, repo, commit):
        # print "weave", repo, commit.id, commit.old_id, commit.message
        self.pending_commits.setdefault(commit.branch, {})[repo, commit.old_id] = commit
        commit.skip(new_id=commit.id)

    def write_commit(self, repo, commit):
        prev_repo, prev_commit_id = self.changed_parents.get((repo, commit.old_id), (None, None))
        if prev_commit_id is not None:
            logging.info("relabeling %s:%s parent from %s:%s -> %s:%s",
                         repo, commit.old_id, repo, commit.from_commit, prev_repo, prev_commit_id)
            commit.from_commit = prev_commit_id
        commit.dump(self.target.stdin if hasattr(self.target, "stdin") else self.target)
        self.written_commits.add(commit.old_id)

    def get_available_commits(self):
        available_commits = []
        for branch, commit_map in self.pending_commits.items():
            # for each branch, look to see if the next commit we need is available
            combined_commits = self.combined_branches.get(branch, [])
            if combined_commits and combined_commits[0] in commit_map:
                repo, commit_id = combined_commits[0]
                if (repo, commit_id) in self.changed_parents:
                    depends_on = self.changed_parents[repo, commit_id][1]
                else:
                    depends_on = commit_map[repo, commit_id].from_commit
                if not depends_on or depends_on in self.written_commits:
                    combined_commits.pop(0)
                    commit = commit_map.pop((repo, commit_id))
                    available_commits.append((branch, repo, commit_id, commit))
                    if not commit_map:
                        del self.pending_commits[branch]
                    if not combined_commits:
                        self.combined_branches.pop(branch)
        return available_commits

    def write_commits(self):
        while self.combined_branches or self.production_in_progress:
            # print "pending_commits", {branch: commit_map.keys() for branch, commit_map in self.pending_commits.items()}
            # print "combined_branches", {branch: combined_commits[0] for branch, combined_commits in self.combined_branches.items() if combined_commits}
            available_commits = self.get_available_commits()
            # print "available_commits", available_commits
            if available_commits:
                for branch, repo, commit_id, commit in available_commits:
                    # print("writing on branch %s repo %s commit_id %s/%s" % (branch, repo, commit_id, commit.id))
                    self.write_commit(repo, commit)
                    # print "production_in_progress", self.production_in_progress
            else:
                print ("No available commits")

    def run_weave(self, repo, export_filter, source, target, id_offset=None):
        try:
            export_filter.run(source, target, id_offset=id_offset)
        finally:
            logging.info("finished repo %s", repo)
            self.production_in_progress.pop(repo)

    def create_merged_export(self):
        self.weave_store = self.open_export_file(basename(self.output_dir), "-weave.git-fast-export", "weaved export")
        self.target = self.weave_store.file
        self.production_in_progress = {n: False for n, store in enumerate(self.exported_stores, start=1)}
        try:
            for n, exported_store in enumerate(self.exported_stores, start=1):
                exported_store.open_for_read()
                try:
                    export_filter = FastExportFilter(reset_callback=lambda r: self.skip_reset(r),
                                                     commit_callback=lambda c: self.weave_commit(n, c))
                    self.production_in_progress[n] = True
                    self.run_weave(n, export_filter, exported_store.file, self.weave_store.file, id_offset=0)
                finally:
                    exported_store.close()
            self.write_commits()
            self.weave_store.close()
        except Exception, e:
            logging.error("Error weaving commits into new repository: %s", e)
            raise

    def import_merged_export(self):
        self.weave_store.open_for_read()
        self.target = fast_import_input(self.output_dir, import_input=self.weave_store.file)
        # Wait for git-fast-import to complete (only necessary since we passed
        # file objects to FastExportFilter.run; and even then the worst that
        # happens is git-fast-import completes after this python script does)
        self.target.wait()
        self.weave_store.close()

    def run(self):
        success = False
        try:
            self.collect_commits()
            self.merge_branches()

            # Reset the _next_id so that it's like we're starting afresh
            _IDS._next_id = 1
            self.create_merged_export()
            self.import_merged_export()
            success = True
        finally:
            if success:
                for store in self.intermediate_files:
                    store.remove_file()
            else:
                for store in self.intermediate_files:
                    store.handle_failure(self.failure_base_dir)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tmpdir", default=tempfile.gettempdir(),
                        help="Override temporary directory for intermediate files")
    parser.add_argument("-T", "--tmplabel", help="Create/reuse temporary files with this label for reuse in debugging")
    parser.add_argument("repos", nargs="+", help="list of repositories to join")
    parser.add_argument("output_repo", help="target repository to create")
    args = parser.parse_args()
    splicer = InterleaveRepositories(args)
    splicer.run()

