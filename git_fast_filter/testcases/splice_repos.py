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
    def __init__(self, label, suffix, description, dir=None, fixed_name=None, overwrite=False):
        self.label = label
        self.description = description
        self.exists = False
        self.in_progress = True
        self.closed = False
        if fixed_name:
            self.filename = os.path.join(dir or tempfile.gettempdir(), "%s-%s%s" % (label, fixed_name, suffix))
            if exists(self.filename) and os.stat(self.filename).st_size > 0 and not overwrite:
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
        self.import_mark_file = join(abspath(self.output_dir), ".git", "splice-mark-import")
        self.woven_branches_filename = join(self.output_dir, ".git", "splice-weave.json")
        self.output_name = basename(self.output_dir)
        self.failure_base_dir = dirname(abspath(self.output_dir))
        self.tmpdir = abspath(args.tmpdir)
        self.tmplabel = args.tmplabel
        self.keep_files = args.keep
        self.intermediate_files = []

        # the head of each branch: branch -> repo -> head commit_id
        self.commit_branch_ends = {}
        # just remember the commit dates for everything
        self.commit_dates = {}
        # commit_parents maps (repo, commit_id) -> (repo, parent_id)
        self.commit_parents = {}
        # combined_branches maps branch -> ordered list of (repo, commit_id)
        self.combined_branches = {}
        # changed_parents maps (repo, commit_id) -> (new_parent_repo, new_parent_id)
        self.changed_parents = {}
        # keeps track of which commit_ids have been written, so we don't write something before its dependencies
        self.written_commit_ids = set()
        # maps (repo, commit_id) -> commit_object for those commits that have yet to be written
        self.pending_commits = {}
        # maps (repo, commit_id) -> subject (first line of message)
        self.commit_subjects = {}
        # maps (repo, commit_id) -> {set of branches}
        self.commit_owners = {}
        # tracks which (repo, commit_id) have been read from a previous splice
        self.archive_commits = set()

    def open_export_file(self, label, suffix, description, overwrite=False):
        file_wrapper = IntermediateFile(label, suffix, description,
                                        dir=self.tmpdir, fixed_name=self.tmplabel, overwrite=overwrite)
        self.intermediate_files.append(file_wrapper)
        return file_wrapper

    def skip_reset(self, reset):
        reset.skip()

    @staticmethod
    def jsonize_commit(commit):
        subject = ''.join(commit.message.strip().splitlines()[:1])
        return (commit.id, commit.branch, datetime_to_json(commit.committer_date), commit.from_commit, subject)

    def remember_previous_commits(self):
        """remembers commits we've previously written from the import marks file"""
        if exists(self.woven_branches_filename):
            with open(self.woven_branches_filename, 'r') as f:
                woven_branches = json.load(f)
                self.restore_woven_branches(woven_branches)

    def save_woven_branches(self):
        return {
            "combined_branches": dict((branch, ["%s:%s" %rc for rc in commits]) for branch, commits in self.combined_branches.items()),
            "commit_parents": dict(("%s:%s" % k, "%s:%s" % v) for k, v in self.commit_parents.items()),
            "changed_parents": dict(("%s:%s" % k, "%s:%s" % v) for k, v in self.changed_parents.items()),
            "commit_owners": dict(("%s:%s" % k, sorted(v)) for k, v in self.commit_owners.items()),
            "commit_dates": dict(("%s:%s" % k, datetime_to_json(d)) for k, d in self.commit_dates.items()),
        }

    def restore_woven_branches(self, woven_branches):
        def _parse_rc_pair(rcs):
            rc = rcs.split(":", 1)
            r = int(rc[0]) if rc[0] != "None" else "None"
            c = int(rc[1]) if rc[1] != "None" else "None"
            return r, c
        prp = _parse_rc_pair
        self.combined_branches = dict((branch, [prp(rcs) for rcs in commits])
                                      for branch, commits in woven_branches["combined_branches"].items())
        self.commit_parents = dict((prp(k), prp(v)) for k, v in woven_branches["commit_parents"].items())
        self.changed_parents = dict((prp(k), prp(v)) for k, v in woven_branches["changed_parents"].items())
        self.commit_owners = dict((prp(k), set(v)) for k, v in woven_branches["commit_owners"].items())
        self.commit_dates = dict((prp(k), json_to_datetime(d)) for k, d in woven_branches["commit_dates"].items())
        self.archive_commits.update(self.commit_owners)

    def remember_commits(self, repo, stored_commits, archive=False):
        for commit_id, commit_branch, committer_date_parts, from_commit, subject in stored_commits:
            committer_date = json_to_datetime(committer_date_parts)
            self.commit_branch_ends.setdefault(commit_branch, {})[repo] = commit_id
            self.commit_parents[repo, commit_id] = (repo, from_commit)
            self.commit_dates[repo, commit_id] = committer_date
            self.commit_subjects[repo, commit_id] = subject
            if archive:
                self.archive_commits.add((repo, commit_id))

    def collect_commits(self):
        self.exported_stores = []
        for repo_num, input_repo in enumerate(self.input_repos, start=1):
            repo_name = basename(input_repo)
            remember_commits_filename = os.path.join(abspath(input_repo), ".git", "splice-commits.json")
            exported_store = self.open_export_file(repo_name, ".git-fast-export", "export", overwrite=True)
            self.exported_stores.append(exported_store)
            logging.info("Doing export on repository %s [%d]", repo_name, repo_num)
            stored_commits = []
            export_mark_file = join(abspath(input_repo), ".git", "splice-export-marks")
            export_mark_args = ["--all", "--export-marks=%s" % export_mark_file]
            if exists(export_mark_file):
                export_mark_args.append("--import-marks=%s" % export_mark_file)
            export = fast_export_output(input_repo, export_mark_args)
            logging.info("_next_id is %d", _IDS._next_id)
            try:
                collect = FastExportFilter(commit_callback=lambda c: stored_commits.append(self.jsonize_commit(c)))
                logging.info("offset for repo %d is %d", repo_num, collect._id_offset)
                kwargs = {}
                if repo_num == 2:
                    kwargs["id_offset"] = 8
                collect.run(export.stdout, exported_store.file, **kwargs)
                exported_store.close()
                with open(remember_commits_filename, 'wb') as remember_commits_file:
                    json.dump(stored_commits, remember_commits_file, indent=4)
            except Exception, e:
                logging.error("Error in memorizing export for %s [%d]: %s", input_repo, repo_num, e)
                raise
            # TODO: handle the fact that marks are getting messed up by the two repos on an incremental import
            self.remember_commits(repo_num, stored_commits)

    def weave_branches(self):
        """for each branch, calculate how combining repos should change parents of commits"""
        logging.info("Weaving %d branches from %d repositories", len(self.commit_branch_ends), len(self.input_repos))
        for branch, repo_heads in self.commit_branch_ends.items():
            self.combined_branches[branch] = combined_commits = []
            repo_branches = {}
            for repo_num, commit_id in sorted(repo_heads.items()):
                committer_date = self.commit_dates[repo_num, commit_id]
                repo_branches[repo_num] = repo_branch_commits = [(committer_date, repo_num, commit_id)]
                previous_commit_id, previous_date = None, None
                while (repo_num, commit_id) in self.commit_parents:
                    _, commit_id = self.commit_parents[repo_num, commit_id]
                    if commit_id is not None:
                        committer_date = self.commit_dates[repo_num, commit_id]
                        if previous_date and committer_date > previous_date:
                            logging.warning("Commit %d:%d on branch %s at %s happened after parent %d:%d at %s",
                                            repo_num, commit_id, branch, committer_date,
                                            repo_num, previous_commit_id, previous_date)
                        repo_branch_commits.append((committer_date, repo_num, commit_id))
                        previous_commit_id, previous_date = commit_id, committer_date
            last_repo, last_commit_id = None, None
            while repo_branches:
                last_commits = [commits[0] for _, commits in sorted(repo_branches.items())]
                committer_date, repo_num, commit_id = max(last_commits)
                commits = repo_branches[repo_num]
                commits.pop(0)
                combined_commits.append((repo_num, commit_id))
                self.commit_owners.setdefault((repo_num, commit_id), set()).add(branch)
                if last_commit_id is not None:
                    parent_repo_num, parent_commit_id = self.commit_parents[last_repo, last_commit_id]
                    if (parent_repo_num, parent_commit_id) != (repo_num, commit_id):
                        if (last_repo, last_commit_id) in self.changed_parents:
                            assigned_repo, assigned_commit_id = self.changed_parents[last_repo, last_commit_id]
                            if (assigned_repo, assigned_commit_id) != (repo_num, commit_id):
                                last = (last_repo, last_commit_id)
                                assigned = (assigned_repo, assigned_commit_id)
                                logging.info("%d:%d on %s at %s with subject %s", last_repo, last_commit_id,
                                             branch, self.commit_dates[last], self.commit_subjects.get(last, "unknown"))
                                logging.info("%d:%d on %s at %s with subject %s", assigned_repo, assigned_commit_id,
                                             branch, self.commit_dates[assigned], self.commit_subjects.get(assigned, "unknown"))
                                logging.info("%d:%d on %s at %s with subject %s", repo_num, commit_id,
                                             branch, committer_date, self.commit_subjects.get((repo_num, commit_id), "unknown"))
                                logging.error("%d:%d on branch %s already maps to %d:%d but must also map to %d:%d",
                                            last_repo, last_commit_id, branch, assigned_repo, assigned_commit_id,
                                            repo_num, commit_id)
                                raise ValueError("Commit %d:%d on branch %s requires two parents" %
                                                 (last_repo, last_commit_id, branch))
                        self.changed_parents[last_repo, last_commit_id] = repo_num, commit_id
                last_repo, last_commit_id = repo_num, commit_id
                if not commits:
                    repo_branches.pop(repo_num)
            logging.info("After processing %s, commit_owners has %d unique entries and %d entries",
                         branch, len(self.commit_owners), sum(len(l) for l in self.commit_owners.values()))
            combined_commits.reverse()
        woven_branches = self.save_woven_branches()
        with open(self.woven_branches_filename, 'wb') as woven_branches_file:
            json.dump(woven_branches, woven_branches_file, indent=4)

    def weave_commit(self, repo, commit):
        # print "weave", repo, commit.id, commit.old_id, commit.message
        self.pending_commits[repo, commit.old_id] = commit
        commit.skip(new_id=commit.id)

    def write_commit(self, repo, commit):
        prev_repo, prev_commit_id = self.changed_parents.get((repo, commit.old_id), (None, None))
        if prev_commit_id is not None:
            logging.debug("relabeling %s:%s parent from %s:%s -> %s:%s",
                          repo, commit.old_id, repo, commit.from_commit, prev_repo, prev_commit_id)
            commit.from_commit = prev_commit_id
        commit.dump(self.target.stdin if hasattr(self.target, "stdin") else self.target)
        # logging.info("Writing commit %s:%s->%s", repo, commit.old_id, commit.id)
        self.written_commit_ids.add(commit.old_id)

    def get_available_commits(self):
        available_commits = []
        for branch, combined_commits in sorted(self.combined_branches.items()):
            # for each branch, look to see if the next commit we need is available
            if not combined_commits:
                self.combined_branches.pop(branch)
                continue
            repo, commit_id = combined_commits[0]
            if (repo, commit_id) in self.changed_parents:
                depends_on = self.changed_parents[repo, commit_id][1]
            else:
                depends_on = self.commit_parents[repo, commit_id][1]
            if not depends_on or depends_on in self.written_commit_ids:
                logging.debug("Found %d:%d to write which belongs to %s", repo, commit_id, ",".join(self.commit_owners[repo, commit_id]))
                for owner_branch in sorted(self.commit_owners[repo, commit_id]):
                    owner_combined_commits = self.combined_branches[owner_branch]
                    owner_combined_commits.remove((repo, commit_id))
                    if not owner_combined_commits:
                        self.combined_branches.pop(owner_branch)
                if (repo, commit_id) in self.pending_commits:
                    commit = self.pending_commits.pop((repo, commit_id))
                else:
                    raise ValueError("couldn't find pending commit %d:%d", repo, commit_id)
                available_commits.append((repo, commit_id, commit))
        return available_commits

    def write_commits(self):
        while self.combined_branches or self.production_in_progress:
            # print "combined_branches", dict((branch, combined_commits[0]) for branch, combined_commits in self.combined_branches.items() if combined_commits)
            available_commits = self.get_available_commits()
            # print "available_commits", available_commits
            if available_commits:
                for repo, commit_id, commit in available_commits:
                    # print("writing on branch %s repo %s commit_id %s/%s" % (branch, repo, commit_id, commit.id))
                    self.write_commit(repo, commit)
                    # print "production_in_progress", self.production_in_progress
            else:
                # this means the algorithm has got stuck and can't resolve the situation
                raise ValueError("No available commits")

    def run_weave(self, repo, export_filter, source, target, id_offset=None):
        try:
            export_filter.run(source, target, id_offset=id_offset)
        finally:
            logging.info("finished repo %s", repo)
            self.production_in_progress.pop(repo)

    def create_woven_export(self):
        self.weave_store = self.open_export_file(basename(self.output_dir), "-weave.git-fast-export", "weaved export", overwrite=True)
        self.target = self.weave_store.file
        self.production_in_progress = dict((repo_num, False) for repo_num, store in enumerate(self.exported_stores, start=1))
        try:
            for repo_num, exported_store in enumerate(self.exported_stores, start=1):
                exported_store.open_for_read()
                try:
                    export_filter = FastExportFilter(reset_callback=lambda r: self.skip_reset(r),
                                                     commit_callback=lambda c: self.weave_commit(repo_num, c))
                    self.production_in_progress[repo_num] = True
                    self.run_weave(repo_num, export_filter, exported_store.file, self.weave_store.file, id_offset=0)
                finally:
                    exported_store.close()
            self.write_commits()
            self.weave_store.close()
        except Exception, e:
            logging.error("Error weaving commits into new repository: %s", e)
            raise

    def import_woven_export(self):
        logging.info("Importing woven result into %s", self.weave_store.filename)
        self.weave_store.open_for_read()
        import_mark_args = ["--import-marks-if-exists=%s" % self.import_mark_file, "--export-marks=%s" % self.import_mark_file]
        self.target = fast_import_input(self.output_dir, import_mark_args, import_input=self.weave_store.file)
        # Wait for git-fast-import to complete (only necessary since we passed
        # file objects to FastExportFilter.run; and even then the worst that
        # happens is git-fast-import completes after this python script does)
        self.target.wait()
        self.weave_store.close()

    def run(self):
        success = False
        try:
            self.remember_previous_commits()
            self.collect_commits()
            if not os.path.isdir(self.output_dir):
                os.makedirs(self.output_dir)
                if subprocess.call(["git", "init", "--shared"], cwd=self.output_dir) != 0:
                    raise SystemExit("git init in %s failed!" % self.output_dir)
            self.weave_branches()

            # Reset the _next_id so that it's like we're starting afresh
            _IDS._next_id = 1
            self.create_woven_export()
            self.import_woven_export()
            success = True
        finally:
            if success:
                for store in self.intermediate_files:
                    if self.keep_files:
                        logging.info("Keeping %s for %s in %s", store.description, store.label, store.filename)
                    else:
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
    parser.add_argument("-k", "--keep", action="store_true", help="Keep intermediate files even on success")
    parser.add_argument("repos", nargs="+", help="list of repositories to join")
    parser.add_argument("output_repo", help="target repository to create")
    args = parser.parse_args()
    weaver = InterleaveRepositories(args)
    weaver.run()

