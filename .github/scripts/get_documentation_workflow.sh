
#! /bin/bash

source ~/.bashrc

 
curl \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $GITHUB_PAT" \
  https://api.github.com/repos/cgauvi/ben.R.utils/actions/workflows 
 
 
curl \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $GITHUB_PAT" \
  https://api.github.com/repos/cgauvi/ben.R.utils/actions/workflows/41536288
 