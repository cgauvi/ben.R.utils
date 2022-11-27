
#! /bin/bash

source ~/.bashrc

 
#-H "Authorization:token ${{ secrets.GH_PAT }}" \
  
curl \
  -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $GITHUB_PAT" \
  https://api.github.com/repos/cgauvi/ben.R.utils/actions/workflows/41536288/dispatches \
  -d '{"ref":"master"}'