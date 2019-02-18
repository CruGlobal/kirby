#!/bin/sh

# print every command before executing
set -ex

git remote set-branches origin 'staging'
git fetch origin --depth=50
git checkout -b staging origin/staging

# attempt a merge
git merge master --no-edit

# record if merge was successful
success=$?

# if merge was unsuccessful, abort the failed merge
if [ ! $success == 0 ] ; then
  git merge --abort
fi

exit $success
