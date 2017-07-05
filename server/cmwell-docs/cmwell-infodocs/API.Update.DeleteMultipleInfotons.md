# Function: *Delete Multiple Infotons* #

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

