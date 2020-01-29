# CM-Well Version Release Notes - Lynx (September 2017) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Kingbird.September.2017.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Mono.November.2017.md)  

----

## Change Summary ##


 Title | Description 
:------|:-----------
LNAV added to package | The LNAV 3rd-party utility for reading log files is now part of the CM-Well package.
New script for ingesting multiple file infotons | Using this script, you can recursively upload to CM-Well all files in the working directory and its descendant folders. See comments in ```CM-Well/server/cmwell-spa/utils/upload.sh ``` to learn more.
UI load time has been reduced | See title
Navigation by type | This feature existed in the old UI, but has only now been added to the new UI. In each view, the types and counts of the results are displayed at the top of the view.
Error code for invalid login credentials | Attempting to login with invalid credentials now produces the 400 error (previously produced no reply).
Enhanced robustness of the upgrade process | The upgrade process robustness was improved in cases of errors in default UI injection.
Cassandra version upgrade | From 1.7.4 to 1.7.6

### Changes to API ###
None.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Kingbird.September.2017.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Mono.November.2017.md)  

----