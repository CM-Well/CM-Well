# Function: *Delete Multiple Infotons* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.DeleteASingleInfoton.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.DeleteFields.md)  

----


## Description ##
You can delete multiple infotons by applying the special #fullDelete indicator to them, using the _in endpoint.

## Syntax ##

**URL:** <cm-well-host>/_in
**REST verb:** POST
**Mandatory parameters:** N/A

----------

**Template:**

    curl -X POST <cm-well-host>/_in?format=<format> <triples to delete>

**URL example:** N/A

**Curl example (REST API):**

    curl -X POST "<cm-well-host>/_in?format=ttl" -H "Content-Type: text/plain" --data-binary @curlInput.txt

### File Contents ###
    <http://data.com/1-11111111> <cmwell://meta/sys#fullDelete> "false" .
    <http://data.com/1-22222222> <cmwell://meta/sys#fullDelete> "false" .
    <http://data.com/1-33333333> <cmwell://meta/sys#fullDelete> "false" .

>**Note:** The "false" value in the triples indicates that the deletion should not be recursive. If a value of "true" is supplied, in addition to the infotons in the request, if there are infotons under the paths supplied in the request, they will be deleted too. Recursive delete may fail if there are too many infoton descendants.

## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Values&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example
:----------|:-------------|:--------|:---------
priority | If this flag appears in an update call, that call receives priority relative to non-priority calls. To use this flag, you must also pass the X-CM-WELL-TOKEN request header, with a token that grants access to the priority feature. This feature should be used with discretion so as not to flood the priority queue. | None | \<cm-well-host\>/_in?format=ttl&priority...

## Code Example ##

### Call ###

    curl -X POST "<cm-well-host>/_in?format=ttl" -H "Content-Type: text/plain" --data-binary @curlInput.txt

### File Contents ###
    <http://data.com/1-11111111> <cmwell://meta/sys#fullDelete> "false" .
    <http://data.com/1-22222222> <cmwell://meta/sys#fullDelete> "false" .
    <http://data.com/1-33333333> <cmwell://meta/sys#fullDelete> "false" .

### Results ###

    {"success":true}

## Notes ##

* CM-Well returns a "success" status code even if the infoton you requested to delete doesn't exist.
* Deleting infotons this way actually just marks them as deleted, but leaves them in CM-Well storage. If you need to permanently remove infotons from CM-Well, see the purge operation.
* Triples containing the #fullDelete indicator may be mixed with other triples for upload.

## Related Topics ##
[Delete a Single Infoton](API.Update.DeleteASingleInfoton.md)
[Purge a Single Infoton](API.Update.Purge.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.DeleteASingleInfoton.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.DeleteFields.md)  

----