#!/bin/bash

git fetch --all --prune &&
git for-each-ref --format='%(refname:short) %(upstream:track)' refs/heads |
  awk '$2 == "[gone]" {print $1}' |
  while IFS= read -r branch; do
    git branch -D -- "$branch"
  done