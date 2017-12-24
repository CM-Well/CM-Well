# Function: *Delete Specific Field Values* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.DeleteFields.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.AddInfotonsAndFieldsToSubGraph.md)  

----

## Description ##

In some cases, an infoton may have several values for the same field name. For example, a Person infoton might have several Telephone or Email field values.

Using the [#markReplace operator](API.Update.DeleteFields.md) to delete a field will delete all of its values. 

If you only want to delete a subset of a field's multiple values, while retaining the other values, there are two ways to do this: 

1. Use the JSON format to define values to delete.
2. Use the special **#markDelete** indicator.

Both methods are described in the following sections.

## Syntax 1 (JSON Format) ##

**URL:** <cm-well-host>/<cm-well-path>
**REST verb:** POST
**Mandatory parameters:** data

----------

**Template:**

    curl -X POST <cm-well-host>/<cm-well-path>?data=<field name and values to delete, in JSON format>

**URL example:** N/A

**Curl example (REST API):**

    curl -X DELETE '<cm-well-host>/example.org/JohnSmith?data={"name":["John","Johnnie"]}'
    
## Parameters 1 ##

Parameter | Description  | Example
:----------|:------------|:-------
data | The field name and values to delete, in JSON format. Values are comma-separated. | {"name":["John","Johnnie"]} 
priority | If this flag appears in an update call, that call receives priority relative to non-priority calls. To use this flag, you must also pass the X-CM-WELL-TOKEN request header, with a token that grants access to the priority feature. This feature should be used with discretion so as not to flood the priority queue. | \<cm-well-host\>/_in?format=ttl&priority...

## Code Example 1 ##

### Call ###

    curl -X DELETE '<cm-well-host>/example.org/JohnSmith?data={"name":["John","Johnnie"]}'

### Results ###

    {"success":true}

## Syntax 2 (#markDelete Indicator) ##

**URL:** <CMWellHost>/_in
**REST verb:** POST
**Mandatory parameters:** N/A

----------

**Template:**

    curl -X POST <cm-well-host>/_in?format=<format> <infotonURI> <cmwell://meta/sys#markDelete> [ <field name and value pairs> ]

**URL example:** N/A

**Curl example (REST API):**

    curl -X POST "<cm-well-host>/_in?format=turtle" --data-binary @curlInput.txt

### File Contents ###
    @prefix vcard: <http://www.w3.org/2006/vcard/ns#> .
    <http://example.org/Individuals/JohnSmith>
    <cmwell://meta/sys#markDelete> [
      vcard:EMAIL <mailto:jane.smith@example.org> ;
      vcard:EMAIL <mailto:jenny.smith@example.org> ]
    
## Parameters 2 ##

Parameter | Description | Values | Example | Reference
:----------|:-------------|:--------|:---------|:----------
format | The format in which the triples are provided | n3, rdfxml, ntriples, nquads, turtle/ttl, trig, trix | format=n3 | [CM-Well Input and Output Formats](API.InputAndOutputFormats.md)
priority | If this flag appears in an update call, that call receives priority relative to non-priority calls. To use this flag, you must also pass the X-CM-WELL-TOKEN request header, with a token that grants access to the priority feature. This feature should be used with discretion so as not to flood the priority queue. | None | \<cm-well-host\>/_in?format=ttl&priority... | [Query Parameters](API.QueryParameters.md)


## Code Example 2 ##

### Call ###

    curl -X POST "<cm-well-host>/_in?format=turtle" --data-binary @curlInput.txt

### File Contents ###

**Triples:**

    @prefix vcard: <http://www.w3.org/2006/vcard/ns#> .
    <http://example.org/Individuals/JohnSmith>
    <cmwell://meta/sys#markDelete> [
      vcard:EMAIL <mailto:jane.smith@example.org> ;
      vcard:EMAIL <mailto:jenny.smith@example.org> ]

**Quads:**

    @prefix vcard: <http://www.w3.org/2006/vcard/ns#> .
    <http://example.org/Individuals/JohnSmith>
    <cmwell://meta/sys#markDelete> [
      vcard:EMAIL <mailto:jane.smith@example.org> <http://mySubGraph>;
      vcard:EMAIL <mailto:jenny.smith@example.org> <http://mySubGraph>]

### Results ###

    {"success":true}

## Notes ##
* You can delete only values with specific sub-graph labels by providing the label value as the 4th value of a field quad (see **Quads** example above).
* If you specify a quad format (e.g. format=nquads) but provide triples data, CM-Well infers <*> as the missing quad value, and therefore will delete all specified fields that have any quad value.
* The [**replace-mode** flag](API.Update.ReplaceFieldValues.md) and the **#markDelete** indicator should not be used in the same command. If they are, **replace-mode** overrides **#markDelete**.

## Related Topics ##
[Delete a Single Infoton](API.Update.DeleteASingleInfoton.md)
[Delete Fields](API.Update.DeleteFields.md)
[Replace Field Values](API.Update.ReplaceFieldValues.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.DeleteFields.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.AddInfotonsAndFieldsToSubGraph.md)  

----