# script_collection_utils

Some of the utility code used to run the script_collection projects.


## 1. insert_with_no_error_commits
From the existing table(scripts_gt, script_las), exclude the error commits in the error2 table and insert them into the new table.

## 2. Compare between two tables
### 2-1. CT_and_ET_match
    *  Get a common file_id with a matching change type and entity type from both target tables.