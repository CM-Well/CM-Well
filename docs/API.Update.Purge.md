# Function: *Purge a Single Infoton's Versions* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.DeleteOrReplaceValuesInNamedSubGraph.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.AddFileInfoton.md)  

----

## Description ##
When you [delete](API.Update.DeleteASingleInfoton.md) an infoton, the operation in fact leaves the infoton in the CM-Well repository, but marks it as deleted. A query on a deleted infoton returns the **DeletedInfoton** status.

In contrast, a **purge** operation physically deletes infoton versions from storage, so they are not accessible afterwards. Trying to access a purged infoton will result in an "Infoton Not Found" error.

>**Note:** Since purge is an irreversible operation, it is not recommended for common use. You would need it only for unusual cases of error in an upload process. 

Purge operations can only be performed on a *single* infoton, and not recursively on a path. If you purge an infoton that has descendant infotons under it, the descendants will remain after the purge operation.

There are three types of purge operations:

* `op=purge-all` - purges all historical versions and the current version of an infoton.
* `op=purge-history` - purges all historical versions of an infoton, but keeps the current version.
* `op=purge-last` (or its alias `op=rollback`) purges the current version of an infoton, and makes its last historical version the current one. If the infoton had no historic versions, the current version is purged.

## Syntax ##

**URL:** Infoton's URI.
**REST verb:** GET
**Mandatory parameters:** N/A

----------

**Template:**

    curl -X GET <cm-well-host>/<InfotonPath>?op=<purge operation>

**URL example:** 

    <cm-well-host>/example.org/Individuals/JohnSmith?op=purge-all
    <cm-well-host>/example.org/Individuals/JohnSmith?op=purge-history
    <cm-well-host>/example.org/Individuals/JohnSmith?op=purge-last

**Curl example (REST API):**

    curl -X GET "<cm-well-host>/example.org/Individuals/JohnSmith?op=purge-all"
    curl -X GET "<cm-well-host>/example.org/Individuals/JohnSmith?op=purge-history"
    curl -X GET "<cm-well-host>/example.org/Individuals/JohnSmith?op=purge-last"
    
## Special Parameters ##

Parameter | Description 
:----------|:-------------
op=purge-all | Requests the purge-all operation
op=purge-history | Requests the purge-history operation
op=purge-last | Requests the purge-last operation


## Code Example ##

### Call ###

    curl -X GET "<cm-well-host>/example.org/Individuals/JohnSmith?op=purge-all"

### Results ###

    {"success":true}

## Notes ##
None.

## Related Topics ##
[Delete a Single Infoton](API.Update.DeleteASingleInfoton.md)
[Delete Multiple Infotons](API.Update.DeleteMultipleInfotons.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.DeleteOrReplaceValuesInNamedSubGraph.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.AddFileInfoton.md)  

----