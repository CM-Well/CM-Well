# Function: *Delete a Single Infoton* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.ReplaceFieldValues.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.DeleteMultipleInfotons.md)  

----

## Description ##
You can delete a single infoton by applying the REST DELETE command to the infoton's URI.

## Syntax ##

**URL:** Infoton's URI.
**REST verb:** DELETE
**Mandatory parameters:** N/A

----------

**Template:**

    curl -X DELETE <cm-well-host>/<cm-well-path>

**URL example:** N/A

**Curl example (REST API):**

    curl -X DELETE <cm-well-host>/example.org/Individuals/JohnSmith

## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Values&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example | Reference
:----------|:-----------------------|:--------|:---------|:----------
recursive | If true, queries and delete command are recursive, i.e. will apply to all infotons who are path-wise descendants of the infotons in the request. (An equivalent but deprecated flag name is `with-descendants`.) | false / true (the default) | recursive=true | N/A

## Code Example ##

### Call ###

    curl -X DELETE <cm-well-host>/example.org/Individuals/JohnSmith

### Results ###

    {"success":true}

## Notes ##

* CM-Well returns a "success" status code even if the infoton you requested to delete doesn't exist.
* Applying the DELETE command to an infoton actually just marks the infoton as deleted, but leaves it in CM-Well storage. If you need to permanently remove infotons from CM-Well, see the purge operation.
* When the **recursive** flag is used, the request may fail if there are too many descendants to delete in a reasonable amount of time.

## Related Topics ##
[Delete Multiple Infotons](API.Update.DeleteMultipleInfotons.md)
[Purge a Single Infoton](API.Update.Purge.md) 


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.ReplaceFieldValues.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.DeleteMultipleInfotons.md)  

----
