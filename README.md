Git Splicing Tool
---------

This project is a set of extensions to the JBoss Tools for SVN to Git migration described below.
These are primarily a tool to combine git repositories with corresponding branches together, with commits spliced in time order, (and with support for incremental imports, but not merges) and also a tool to filter git repositories

*We have no intention to maintain or improve this tool.*

The git-splice tool (under `git_fast_filter/test_cases/splice_repos.py`) worked effectively for us during the period we were preparing to migrate from two different svn repositories to a single git repository, but has a few bugs - it sometimes mismatches marks and breaks the incremental import. We managed to survive until we'd completed the migration but I'd recommend testing before using this.

The git filter tool (under `git_fast_filter/test_cases/renamer.py`) allows renaming and excluding files and branches, which can help with the splicing process, or may be useful independently. It is fairly well tested and I'm not aware of any known bugs

Scripts for doing JBoss Tools SVN to Git migration.
---------

This repo contains a copy/fork of git://gitorious.org/git_fast_filter/mainline.git wit 
addition of a set of .py scripts used to perform the migration.

See FAQ.md for questions concerning the migration.


1) git clone mirror repo to get all branches lcoal and use the
readonly url to avoid being able to mirror back.

      $ git clone git://github.com/jbosstools/jbosstools-full-svn-mirror.git
      $ cd jbosstools-svn-mirro && git pull --all

2) Backup local repo for reuse later

      $ tar -czvf jbosstools-full-svn-mirror.tar.gz jbosstools-full-svn-mirror
 
   To get a clean copy:

      $ tar -xvf jbosstools-full-svn-mirror.tar.gz
      
3) Enter the new copy folder, and clean out all but the content we care about

      $ cd jbosstools-svn-mirror
      # get latest changes (if svn still active)
      $ git pull 
      # rename trunk to master to follow git naming convention plus indicate this is not a svn mirror. 
      $ git branch -m trunk master
      $ git branch -rd origin/trunk

      # delete the branches with "dead/experimental" stuff
      $ git branch -a -r | grep -E "jbpm-jpdl-4.0.0.beta2|https|origin/jbosstools-4.0.0.Alpha1|tags/jbosstools-3.0.x|3.2.helios|3.3.indigo|3.1.0.M3|vpe-spring|xulrunner-1.9.2.16|hibernatetools|dead|smooks|tycho_exp|modular_build" | xargs git branch -r -D 
      # delete old branches/tags.
      $ git branch -a -r | grep -vE "GA" | grep -v "\.x" | grep -vi "final" | grep -v "jbosstools-4" | grep -v jbpm-jpdl | grep -v trunk | xargs git branch -r -D
      # delete ambigious/not used branches
      $ git branch -D jbosstools-4.0.0.Alpha2

4) Checkout all branches locally so not just tracked remotely:

      $ git branch -a | sed -e "s/remotes.origin\///g" | xargs -I {} git branch {} remotes/origin/{}

5) Convert branch svn tags into real tags

      # make a tag of each branch in tags/*
      $      git branch | grep tags | sed -e "s/tags\///g" | xargs -n 1 -I {} git tag -m "svn branch tag" {} tags/{}
      # delete all branches in tags/*
      $ git branch | grep tags | xargs -n 1 git branch -d 
      # you can ignore warning: deleting branch '<tagname>' that has been merged to '<tagname>', but not yet been merged to HEAD.
  
6) Remove reference to the origin remote

      $ git remote rm origin

7) setup git_fast_filter, GitHub credentials etc.

      ## edit setup.sh to set username/password/roots if defaults not acceptable.

      $ cd ..
      $ source setup.sh

8) Run the split/filter of repositories (this requires *alot* of disk space)

   $ ./filter-repos.sh

   Each repo is done by running filter_repo.py like this:
   
       $ python filter_repo.py jbosstools-svn-mirror jbosstools-base "^common.*|^tests.*|^runtime.*|^usage.*"

   And then master is checked out, garbage collected, removed empty
   commits, each repo with just one subdir is filter-branched to have
   the subdir as root and finally a second garbage collection.
   
   Done by filter-repos.sh:

    $ echo xxx Checking out master.....
    $ git checkout master
    $ deleteemptybranches.sh $GIT_WORK_TREE
    $ git reset --hard && git for-each-ref --format="%(refname)" refs/original/ | xargs -n 1 git update-ref -d && git reflog expire --expire=now --all && git gc --aggressive --prune=now
    $ git filter-branch --tag-name-filter cat --prune-empty -- --all
 
9) Delete big files (for anything missed in the above steps, it's much cheaper to do this during filter_repo!)

      # Make sure you have all branches/tags pulled (git clone is not sufficient) 
      git pull --all
      # (same as step 4)
      $ git branch -a | sed -e "s/remotes.origin\///g" | xargs -I {} git branch {} remotes/origin/{}
      
      # locate big files
      
      git verify-pack -v .git/objects/pack/pack-*.idx | grep blob | sort -k3nr | head | while read s x b x; do git rev-list --all --objects | grep $s | awk '{print "'"$b"'",$0;}'; done

      # filterbranch to remove the file(s)
      
      git filter-branch --prune-empty -d /dev/shm/scratch --index-filter "git rm --cached -f --ignore-unmatch oops.iso" --tag-name-filter cat -- --all
      
      # garbage collect the repository (destructive)
      $ repo_gc.sh
      
9a) (Missed in our original migration) [Fix line endings][] - can be done later.

    See 
    $ echo "* text=auto" >>.gitattributes
    $ rm .git/index     # Remove the index to force git to
    $ git reset         # re-scan the working directory
    $ git status        # Show files that will be normalized
    $ git add -u
    $ git add .gitattributes
    $ git commit -m "Introduce end-of-line normalization"

10) Create github repos

      $ find jbosstools-* -maxdepth 0 | xargs -n 1 -I {} createrepo.sh {}

      # Delete repos (if needed)
      $ find jbosstools-* -maxdepth 0 | xargs -n 1 -I {} curl -u "maxandersen:$GITHUBPWD" https://api.github.com/$GITHUB_ROOT/scratch-{} -X DELETE

11) Push changes

      # You might need to use --force here if the destination repo is new or contain old history.
      # Push all branches
      $ git push --all  
      # Push all tags
      $ git push --tags
      
      
Resources used:

  * [GitHub API][] - GitHub REST API allowed me to setup and destroy multiple repositories very easily. Not having to click through the web ui safed me a lot of time. 
  * [git_fast_filter][] - Git Fast Filter is several magnitudes faster than using git filter-branch. Highly recommended for splitting up a git repository.
  * [Subversion replication at Atlassian][] - Page describing in detail svn mirroring
  * [Atlassian SVN to Git Migration][] - Page describing how Atlassian migrated by using a svn mirror, sync and git svn fetch.
  * [Using tmpfs with filter_branch][] - If you have to use filter-branch then use it together with a memory mapped filesystem for speed reasons
  * [ramdisk for OSX][] - Scripts to create a memory mapped filesystem on OSX
  * [Clean out empty commits][] - empty commits occur often when commits has no file in them because of the filter or if changes only relate to svn props. Makes the history messy.
  * [Detach subdirectory into separate git repository][] - 
  * [Purge huge files from history][] - Be careful; this rewrites history
  * [Fix line endings][] - Explain methods to fix/unify lineendings in your repository

[Github API]: http://developer.github.com/v3/ "Github REST API"
[git_fast_filter]: gitorious.org/git_fast_filter "Git fast filter"
[Atlassian SVN to Git Migration]: http://blogs.atlassian.com/2012/01/moving-confluence-from-subversion-to-git
[Using tmpfs with filter_branch]: http://debuggable.com/posts/muscles-on-demand-clean-a-large-git-repository-the-cloud-way:49ba8538-d7ac-486d-b132-0cce4834cda3
[ramdisk for OSX]: https://gist.github.com/822455
[Clean out empty commits]: http://stackoverflow.com/questions/7067015/svn2git-with-exclude-any-way-to-ignore-the-empty-blank-commits?lq=1
[Detach subdirectory into separate git repository]: http://stackoverflow.com/questions/359424/detach-subdirectory-into-separate-git-repository
[Purge huge files from history]: http://stackoverflow.com/questions/2100907/how-do-i-purge-a-huge-file-from-commits-in-git-history
[Fix line endings]: http://stackoverflow.com/questions/1510798/trying-to-fix-line-endings-with-git-filter-branch-but-having-no-luck
[Subversion replication at Atlassian]: http://blogs.atlassian.com/2008/11/subversion_replication_at_atla/



Svn Sync notes: 
svnsync sync file:///home/svnsync/repos/jbosstools-mirror

