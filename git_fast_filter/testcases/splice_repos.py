#!/usr/bin/env python

import os
import shutil
import sys
import logging
import tempfile
from os.path import abspath, basename, dirname, join

from git_fast_filter import Reset, Commit, FastExportFilter, record_id_rename
from git_fast_filter import fast_export_output, fast_import_input
from git_fast_filter import _IDS

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

        self.commit_branches = {}
        self.commit_parents = {}
        self.combined_branches = {}
        self.changed_parents = {}
        self.last_commit = None
        self.memory_check = {}
        self.written_commits = set()
        self.pending_commits = {}

    def skip_reset(self, reset):
        reset.skip()

    def remember_commits(self, repo, commit):
        # print "remember", repo, commit.id, commit.old_id, commit.message
        self.commit_branches.setdefault(commit.branch, {}).setdefault(repo, []).append(
            (commit.committer_date, repo, commit.id))
        self.commit_parents[repo, commit.id] = (repo, commit.from_commit)

    def merge_branches(self):
        for branch, repo_maps in self.commit_branches.items():
            self.combined_branches[branch] = combined_commits = []
            branch_maps = [repo_maps[repo] for repo in sorted(repo_maps)]
            last_repo, last_commit_id = None, None
            while branch_maps:
                next_commits = [branch_map[0] for branch_map in branch_maps]
                committer_date, repo, commit_id = min(next_commits)
                branch_map = repo_maps[repo]
                branch_map.pop(0)
                combined_commits.append((repo, commit_id))
                if last_commit_id is not None and self.commit_parents[repo, commit_id] != (last_repo, last_commit_id):
                    self.changed_parents[repo, commit_id] = last_repo, last_commit_id
                last_repo, last_commit_id = repo, commit_id
                if not branch_map:
                    repo_maps.pop(repo)
                    branch_maps = [repo_maps[repo] for repo in sorted(repo_maps)]

    def write_commit(self, repo, commit):
        # print "write", repo, commit.old_id
        prev_repo, prev_commit_id = self.changed_parents.get((repo, commit.old_id), (None, None))
        if prev_commit_id is not None:
            # print "relabeling parent %s -> %s:%s" % (commit.from_commit, prev_repo, prev_commit_id)
            commit.from_commit = prev_commit_id
        commit.dump(self.target.stdin if hasattr(self.target, "stdin") else self.target)
        self.written_commits.add(commit.old_id)

    def get_available_commits(self):
        available_commits = []
        for branch, commit_map in self.pending_commits.items():
            combined_commits = self.combined_branches.get(branch, [])
            if combined_commits and combined_commits[0] in commit_map:
                repo, commit_id = combined_commits[0]
                depends_on = commit_map[repo, commit_id].from_commit
                if depends_on and depends_on not in self.written_commits:
                    continue
                else:
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

    def weave_commit(self, repo, commit):
        # print "weave", repo, commit.id, commit.old_id, commit.message
        self.pending_commits.setdefault(commit.branch, {})[repo, commit.old_id] = commit
        commit.skip(new_id=commit.id)

    def run_weave(self, repo, export_filter, source, target, id_offset=None):
        try:
            export_filter.run(source, target, id_offset=id_offset)
        finally:
            print "finished repo", repo
            self.production_in_progress.pop(repo)

    def run(self):
        input1 = fast_export_output(self.repo1)
        input2 = fast_export_output(self.repo2)
        store1 = TemporaryOutput(basename(self.repo1), ".git-fast-export", "export")
        store2 = TemporaryOutput(basename(self.repo2), ".git-fast-export", "export")
        failure_base_dir = dirname(abspath(self.output_dir))
        success = False
        try:
            collect1 = FastExportFilter(commit_callback=lambda c: self.remember_commits(1, c))
            collect1.run(input1.stdout, store1.file)
            collect2 = FastExportFilter(commit_callback=lambda c: self.remember_commits(2, c))
            collect2.run(input2.stdout, store2.file)
            success = True
        except Exception, e:
            logging.error("Error in memorizing export: %s", e)
            raise
        finally:
            store1.close()
            store2.close()
            if not success:
                store1.handle_failure(failure_base_dir, in_progress=True)
                store2.handle_failure(failure_base_dir, in_progress=True)
        self.merge_branches()

        # Reset the _next_id so that it's like we're starting afresh
        _IDS._next_id = 1
        store1.open_for_read()
        store2.open_for_read()
        success = False
        weave_store = TemporaryOutput(basename(self.output_dir), "-weave.git-fast-export", "weaved export")
        self.target = weave_store.file
        try:
            filter1 = FastExportFilter(reset_callback=lambda r: self.skip_reset(r),
                                       commit_callback=lambda c: self.weave_commit(1, c))
            self.production_in_progress = {1: True, 2: True}
            self.run_weave(1, filter1, store1.file, weave_store.file, id_offset=0)

            filter2 = FastExportFilter(reset_callback=lambda r: self.skip_reset(r),
                                       commit_callback=lambda c: self.weave_commit(2, c))
            self.run_weave(2, filter2, store2.file, weave_store.file, id_offset=0)

            self.write_commits()
            success = True
        except Exception, e:
            logging.error("Error weaving commits into new repository: %s", e)
            raise
        finally:
            store1.close()
            store2.close()
            weave_store.close()
            if not success:
                store1.handle_failure(failure_base_dir, in_progress=False)
                store2.handle_failure(failure_base_dir, in_progress=False)
                weave_store.handle_failure(failure_base_dir, in_progress=True)

        success = False
        weave_store.open_for_read()
        try:
            self.target = fast_import_input(self.output_dir)
            self.target.stdin.writelines(weave_store.file.readlines())
            # Wait for git-fast-import to complete (only necessary since we passed
            # file objects to FastExportFilter.run; and even then the worst that
            # happens is git-fast-import completes after this python script does)
            self.target.stdin.close()
            self.target.wait()
            success = True
        finally:
            weave_store.close()
            if success:
                store1.remove_file()
                store2.remove_file()
                weave_store.remove_file()
            else:
                store1.handle_failure(failure_base_dir, in_progress=False)
                store2.handle_failure(failure_base_dir, in_progress=False)
                weave_store.handle_failure(failure_base_dir, in_progress=False)

if __name__ == '__main__':
    splicer = InterleaveRepositories(sys.argv[1], sys.argv[2], sys.argv[3])
    splicer.run()

