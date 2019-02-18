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
  curl -X POST -H "Content-Type: application/json" \
      -d '{"message":"Failed to merge master into staging on CruGlobal/kirby! Fix it soon before another developer ties to merge their branch into staging!"}' \
      http://c4e22262d9bcf22b2db37928a534665a.balena-devices.com:8080/hubot/send_to/${NOTIFY_ROOM}
fi

exit $success
