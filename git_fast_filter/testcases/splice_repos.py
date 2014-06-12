#!/usr/bin/env python

import argparse
import os
import shutil
import sys
import logging
import tempfile
from os.path import abspath, basename, dirname, join

from git_fast_filter import Reset, Commit, FastExportFilter, record_id_rename
from git_fast_filter import fast_export_output, fast_import_input
from git_fast_filter import _IDS

# TODO: allow incremental update
# TODO: make this a command-line option
KEEP_ON_ERROR = True

class TemporaryOutput(object):
    def __init__(self, label, suffix, description):
        self.label = label
        self.description = description
        self.fd, self.filename = tempfile.mkstemp(prefix="%s-" % label, suffix=suffix)
        self.file = os.fdopen(self.fd, 'wb')

    def close(self):
        self.file.close()

    def open_for_read(self):
        self.file = open(self.filename, "rb")

    def handle_failure(self, target_directory, in_progress=True):
        status = "in-progress " if in_progress else ""
        if not KEEP_ON_ERROR:
            try:
                os.remove(self.filename)
            except Exception, e:
                logging.warning("Error removing temporary file %s", self.filename)
        else:
            filename = join(target_directory, basename(self.filename))
            logging.warning("Storing %s%s for %s in %s", status, self.description, self.label, filename)
            shutil.move(self.filename, filename)

    def remove_file(self):
        try:
            os.remove(self.filename)
        except Exception, e:
            logging.warning("Error removing temporary file %s", self.filename)

class InterleaveRepositories:
    def __init__(self, repo1, repo2, output_dir):
        self.repo1 = repo1
        self.repo2 = repo2
        self.output_dir = output_dir
        self.failure_base_dir = dirname(abspath(self.output_dir))

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

    def skip_reset(self, reset):
        reset.skip()

    def remember_commits(self, repo, commit):
        self.commit_branches.setdefault(commit.branch, {}).setdefault(repo, []).append(
            (commit.committer_date, repo, commit.id))
        self.commit_parents[repo, commit.id] = (repo, commit.from_commit)
        self.commit_dates[repo, commit.id] = commit.committer_date

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

    def collect_commits(self):
        input1 = fast_export_output(self.repo1)
        input2 = fast_export_output(self.repo2)
        self.store1 = TemporaryOutput(basename(self.repo1), ".git-fast-export", "export")
        self.store2 = TemporaryOutput(basename(self.repo2), ".git-fast-export", "export")
        self.in_progress_stores += [self.store1, self.store2]
        try:
            collect1 = FastExportFilter(commit_callback=lambda c: self.remember_commits(1, c))
            collect1.run(input1.stdout, self.store1.file)
            collect2 = FastExportFilter(commit_callback=lambda c: self.remember_commits(2, c))
            collect2.run(input2.stdout, self.store2.file)
        except Exception, e:
            logging.error("Error in memorizing export: %s", e)
            raise
        finally:
            self.store1.close()
            self.in_progress_stores.remove(self.store1)
            self.completed_temporary_stores.append(self.store1)
            self.store2.close()
            self.in_progress_stores.remove(self.store2)
            self.completed_temporary_stores.append(self.store2)

    def create_merged_export(self):
        self.store1.open_for_read()
        self.store2.open_for_read()
        self.weave_store = TemporaryOutput(basename(self.output_dir), "-weave.git-fast-export", "weaved export")
        self.in_progress_stores.append(self.weave_store)
        self.target = self.weave_store.file
        try:
            filter1 = FastExportFilter(reset_callback=lambda r: self.skip_reset(r),
                                       commit_callback=lambda c: self.weave_commit(1, c))
            self.production_in_progress = {1: True, 2: True}
            self.run_weave(1, filter1, self.store1.file, self.weave_store.file, id_offset=0)

            filter2 = FastExportFilter(reset_callback=lambda r: self.skip_reset(r),
                                       commit_callback=lambda c: self.weave_commit(2, c))
            self.run_weave(2, filter2, self.store2.file, self.weave_store.file, id_offset=0)

            self.write_commits()
        except Exception, e:
            logging.error("Error weaving commits into new repository: %s", e)
            raise
        finally:
            self.store1.close()
            self.store2.close()
            self.weave_store.close()
            self.in_progress_stores.remove(self.weave_store)
            self.completed_temporary_stores.append(self.weave_store)

    def import_merged_export(self):
        self.weave_store.open_for_read()
        try:
            self.target = fast_import_input(self.output_dir, import_input=self.weave_store.file)
            # Wait for git-fast-import to complete (only necessary since we passed
            # file objects to FastExportFilter.run; and even then the worst that
            # happens is git-fast-import completes after this python script does)
            self.target.wait()
        finally:
            self.weave_store.close()

    def run(self):
        self.in_progress_stores = []
        self.completed_temporary_stores = []
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
                for store in self.completed_temporary_stores:
                    store.remove_file()
            else:
                for store in self.in_progress_stores:
                    store.close()
                    store.handle_failure(self.failure_base_dir, in_progress=True)
                for store in self.completed_temporary_stores:
                    store.handle_failure(self.failure_base_dir, in_progress=False)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("repos", nargs=2, help="list of repositories to join")
    parser.add_argument("output_repo", help="target repository to create")
    args = parser.parse_args()
    splicer = InterleaveRepositories(args.repos[0], args.repos[1], args.output_repo)
    splicer.run()

