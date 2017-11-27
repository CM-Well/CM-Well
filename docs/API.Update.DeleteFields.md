# Function: *Delete Fields* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.DeleteMultipleInfotons.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.DeleteSpecificFieldValues.md)  

----

## Description ##
You can delete one or more infoton fields by applying the special #markReplace indicator to them, using the _in endpoint.

You specify the field to delete using this triple format:

    <infotonURI> <cmwell://meta/sys#markReplace> <fieldID>

You can also use the wildcard <*> instead of a field ID, as follows:

    <infotonURI> <cmwell://meta/sys#markReplace> <*>

This will delete all the infoton's fields that belong to the default graph. Fields added with a specific sub-graph label will not be deleted by this command.

You can also delete fields with specific sub-graph labels by supplying the label as the 4th value in a quad, as follows:

    <infotonURI> <cmwell://meta/sys#markReplace> <fieldID> <graphLabel>

And you can delete all fields by supplying the <*> wildcard as the 4th value in the quad, as follows:

    <infotonURI> <cmwell://meta/sys#markReplace> <fieldID> <*>

## Syntax ##

**URL:** <CMWellHost>/_in
**REST verb:** POST
**Mandatory parameters:** N/A

----------

**Template:**

    curl -X POST <cm-well-host>/_in?format=<format> <triples or quads to delete>

**URL example:** N/A

**Curl example (REST API):**

**Triples:**

    curl -X POST "<cm-well-host>/_in?format=ttl " -H "Content-Type: text/plain" --data-binary '<http://data.com/1-12345678> 
    <cmwell://meta/sys#markReplace> <http://ont.com/bermuda/hasName> .'

**Quads:**

    curl -X POST "<cm-well-host>/_in?format=nquads" -H "Content-Type: text/plain" --data-binary '<http://data.com/1-12345678> 
    <cmwell://meta/sys#markReplace> <http://ont.com/bermuda/hasName> <http://mySubGraph>.'

**All quad values with sub-graph labels:**

    curl -X POST "<cm-well-host>/_in?format=nquads" -H "Content-Type: text/plain" --data-binary '<http://data.com/1-12345678> 
    <cmwell://meta/sys#markReplace> <http://ont.com/bermuda/hasName> <*>.'

## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Values&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example
:----------|:-------------|:--------|:---------
priority | If this flag appears in an update call, that call receives priority relative to non-priority calls. To use this flag, you must also pass the X-CM-WELL-TOKEN request header, with a token that grants access to the priority feature. This feature should be used with discretion so as not to flood the priority queue. | None | \<cm-well-host\>/_in?format=ttl&priority...

## Code Example ##

### Call ###

    curl -X POST "<cm-well-host>/_in?format=ttl " -H "Content-Type: text/plain" --data-binary '<http://data.com/1-12345678> 
    <cmwell://meta/sys#markReplace> <http://ont.com/bermuda/hasName> .'

### Results ###

    {"success":true}

## Notes ##

* You can also use the #markReplace operator to replace existing field values rather than delete them. See [Replace Field Values](API.Update.ReplaceFieldValues.md).
* The operation described above deletes all values of a field. If you want to delete only a subset of multiple field values, see [Delete Specific Field Values](API.Update.DeleteSpecificFieldValues.md).
* The [**replace-mode** flag](API.Update.ReplaceFieldValues.md) and the **#markReplace** indicator should not be used in the same command. If they are, **replace-mode** overrides **#markReplace**.

## Related Topics ##
[Delete a Single Infoton](API.Update.DeleteASingleInfoton.md)
[Delete Specific Field Values](API.Update.DeleteSpecificFieldValues.md)
[Replace Field Values](API.Update.ReplaceFieldValues.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.DeleteMultipleInfotons.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.DeleteSpecificFieldValues.md)  

----