# CM-Well Version Release Notes - Dolphin (Mar. 2017) #

## Change Summary ##

This version mostly addresses bug fixes, performance and stability.

GitLab Item # | Title | Description
:-------------|:------|:-----------
458 | Remove version number from web UI | We have decided to remove the numeric version label from the CM-Well web UI to prevent confusion, since versions now have textual names.
445 | Consumer exception for bad data chunk | This bug would sometimes occur when using the **consume** API. If an invalid data chunk was encountered, the HTTP error 500 (Internal Server Error) was produced and the consume operation would fail at that point. After the fix, in this case the 206 error (Partial Content) is produced, and the consume stream continues with the next valid data chunk. (This fix is important for the Data Fusion user.)

### Changes to API ###
N/A