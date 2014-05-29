#!/usr/bin/env python

import os
import re
import sys
import logging
import tempfile

from git_fast_filter import Reset, Commit, FastExportFilter, record_id_rename
from git_fast_filter import fast_export_output, fast_import_input
from git_fast_filter import _IDS

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
    self.pending_commits = {}

  def skip_reset(self, reset):
    reset.skip()

  def remember_commits(self, repo, commit):
    # print "remember", repo, commit.id, commit.old_id, commit.message
    self.commit_branches.setdefault(commit.branch, {}).setdefault(repo, []).append((commit.committer_date, repo, commit.id))
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
    commit.dump(self.target.stdin)

  def get_available_commits(self):
    available_commits = []
    for branch, commit_map in self.pending_commits.items():
      combined_commits = self.combined_branches.get(branch, [])
      if combined_commits and combined_commits[0] in commit_map:
        repo, commit_id = combined_commits.pop(0)
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
    commit.skip(new_id = commit.id)

  def run_weave(self, repo, export_filter, source, target, id_offset=None):
    try:
      export_filter.run(source, target, id_offset=id_offset)
    finally:
      print "finished repo", repo
      self.production_in_progress.pop(repo)

  def run(self):
    input1 = fast_export_output(self.repo1)
    input2 = fast_export_output(self.repo2)
    store1_fd, store1_filename = tempfile.mkstemp(suffix=".git-fast-export")
    store2_fd, store2_filename = tempfile.mkstemp(suffix=".git-fast-export")
    store1, store2 = os.fdopen(store1_fd, 'wb'), os.fdopen(store2_fd, 'wb')
    try:
        try:
            collect1 = FastExportFilter(commit_callback = lambda c: self.remember_commits(1, c))
            collect1.run(input1.stdout, store1)
            collect2 = FastExportFilter(commit_callback = lambda c: self.remember_commits(2, c))
            collect2.run(input2.stdout, store2)
        finally:
            store1.close()
            store2.close()
    except Exception, e:
        try:
            os.remove(store1_filename)
        except Exception, e:
            logging.warning("Error removing temporary file %s", store1_filename)
        try:
            os.remove(store2_filename)
        except Exception, e:
            logging.warning("Error removing temporary file %s", store2_filename)
    self.merge_branches()

    # Reset the _next_id so that it's like we're starting afresh
    _IDS._next_id = 1
    store1, store2 = open(store1_filename, 'rb'), open(store2_filename, 'rb')
    try:
        self.target = fast_import_input(self.output_dir)
        # import collections
        # self.target = collections.namedtuple('dummy', 'stdin')(open("/tmp/x", "wb")) # (tempfile.TemporaryFile())
        filter1 = FastExportFilter(reset_callback  = lambda r: self.skip_reset(r),
                                   commit_callback = lambda c: self.weave_commit(1, c))
        self.production_in_progress = {1: True, 2: True}
        self.run_weave(1, filter1, store1, self.target.stdin, id_offset=0)

        filter2 = FastExportFilter(reset_callback  = lambda r: self.skip_reset(r),
                                   commit_callback = lambda c: self.weave_commit(2, c))
        self.run_weave(2, filter2, store2, self.target.stdin, id_offset=0)

        self.write_commits()
        # Wait for git-fast-import to complete (only necessary since we passed
        # file objects to FastExportFilter.run; and even then the worst that
        # happens is git-fast-import completes after this python script does)
        self.target.stdin.close()
        self.target.wait()
    finally:
        store1.close()
        store2.close()
        try:
            os.remove(store1_filename)
        except Exception, e:
            logging.warning("Error removing temporary file %s", store1_filename)
        try:
            os.remove(store2_filename)
        except Exception, e:
            logging.warning("Error removing temporary file %s", store2_filename)

splicer = InterleaveRepositories(sys.argv[1], sys.argv[2], sys.argv[3])
splicer.run()

