#!/bin/bash

set -eux

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
CURRENT_COMMIT=$(git rev-parse HEAD)

cd $SCRIPT_DIR/..
source venv/bin/activate
git checkout gh-pages
git reset --hard $CURRENT_COMMIT
make docs
git add -f docs
git commit -m "Build docs"
git push -f origin gh-pages
git checkout $CURRENT_BRANCH
