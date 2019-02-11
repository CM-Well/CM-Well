# Function: *Add a Link Infoton* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.AddFileInfoton.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.TrackUpdates.md)  

----

## Description ##

A link infoton is a special type of infoton, which behaves similarly to a redirected URL. It points to another infoton (the target infoton). Querying a link infoton returns the same results as querying the target infoton.

For example, if company A has acquired company B, you might want to create a temporary or permanent link infoton, with the original URI of company B, but which points (via redirection) to the company A object.

There are two ways to add a file to CM-Well: 

1. A POST to the `_in` endpoint, using the **LinkInfoton** metatag.
2. A POST to the desired link path, using the **X-CM-WELL-TYPE:LINK** header.

Both methods are described in the following sections.

>**Note:** When adding a link infoton, you can use either the POST or the PUT REST verbs; their behavior is the same.

## Syntax 1 ##

**URL:** \<cm-well-host\>/_in 
**REST verb:** POST
**Mandatory parameters:** The link as a triple subject, and **type**, **linkTo** and **linkType** objects (see template).

----------

**Template:**

    curl -X POST "<cm-well-host>/_in?format=<format>" --data-binary @curlInput.txt

### File Contents ###
    <LinkPath> <cmwell://meta/sys#type> "LinkInfoton" .
    <LinkPath> <cmwell://meta/sys#linkTo> <TargetInfotonPath> .
    <LinkPath> <cmwell://meta/sys#linkType> "<LinkType>"^^<http://www.w3.org/2001/XMLSchema#int> .

**URL example:** N/A

**Curl example (REST API):**

    curl -X POST "<cm-well-host>/in?format=ttl" --data-binary @curlInput.txt

### File Contents ###
    <http://people/BillSmith> <cmwell://meta/sys#type> "LinkInfoton" .
    <http://people/BillSmith> <cmwell://meta/sys#linkTo> <http://people/WilliamSmith> .
    <http://people/BillSmith> <cmwell://meta/sys#linkType> "1"^^<http://www.w3.org/2001/XMLSchema#int> .

## Parameters 1 ##

Parameter | Description | Values 
:----------|:-------------|:--------
**type** triple | A triple that indicates that this is a link infoton | Constant: "LinkInfoton" 
**linkTo** triple | A triple that defines the target infoton | The URI of an existing infoton
**linkType** triple | A triple that indicates the link type | 0 - Permanent link <br>1 - Temporary link <br>2 - Forward link

## Code Example 1 ##

### Call ###

    curl -X POST "<cm-well-host>/in?format=ttl" --data-binary @curlInput.txt

### File Contents ###
    <http://people/BillSmith> <cmwell://meta/sys#type> "LinkInfoton" .
    <http://people/BillSmith> <cmwell://meta/sys#linkTo> <http://people/WilliamSmith> .
    <http://people/BillSmith> <cmwell://meta/sys#linkType> ""^^<http://www.w3.org/2001/XMLSchema#int> .

### Results ###

    {"success":true}

## Syntax 2 ##

**URL:** <CMWellHost>/_in
**REST verb:** POST
**Mandatory parameters:** link infoton path, target infoton path, **X-CM-WELL-TYPE:LN** header/value.

----------

**Template:**

    curl -X POST <cm-well-host>/linkInfotonPath -H "X-CM-WELL-TYPE:LN" -d <targetInfotonPath> -H "X-CM-WELL-LINK-TYPE:2"

>**Note:** The **X-CM-WELL-LINK-TYPE** header is optional. See **Parameters** section to learn about its values.

**URL example:** N/A

**Curl example (REST API):**

    curl -X POST <cm-well-host>/example.org/JohnnySmith -H "X-CM-WELL-TYPE:LN" -H "X-CM-WELL-LINK-TYPE:2" -d "http://example.org/JohnSmith"

## Parameters 2 ##

Parameter | Description | Values 
:----------|:-------------|:--------
X-CM-WELL-TYPE | Indicates that we are adding a link infoton | Constant: "LN" 
X-CM-WELL-LINK-TYPE | An optional header that specifies the link type. If not specified, the default value is 1. | 0 - Permanent link <br>1 - Temporary link <br>2 - Forward link

>**Note:** A GET call to a link infoton produces an HTTP status code of 301 (Moved Permanently) for a permanent link, 307 (Moved Temporarily) for a temporary link, and the status of the target infoton for a forward link.

## Code Example 2 ##

### Call ###

    curl -X POST <cm-well-host>/example.org/JohnnySmith -H "X-CM-WELL-TYPE:LN" -H "X-CM-WELL-LINK-TYPE:2" -d "http://example.org/JohnSmith"

### Results ###

    {"success":true}

## Notes ##

None.

## Related Topics ##
None.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.AddFileInfoton.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.TrackUpdates.md)  

----