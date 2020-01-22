# Function: *Add a File Infoton* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.Purge.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.AddLinkInfoton.md)  

----

## Description ##
In addition to infotons used for data modeling, CM-Well allows you to add files as infotons. The file can be of any type, and it can have field values just like any other infoton. The file contents are stored on CM-Well in addition to the field values.

For example, you may want to store documentation or image files on CM-Well, which describe your CM-Well data model.

There are two ways to add a file to CM-Well: 

1. A simple POST command that takes a pointer to the added file.
2. A POST to the _in endpoint that takes RDF triples, allowing you to add field values in the same call.

Both methods are described in the following sections.

>**Note:** When adding a file infoton, you can use either the POST or the PUT REST verbs; their behavior is the same.
## Syntax 1 ##

**URL:** URI of the file in its CM-Well path. 
**REST verb:** POST
**Mandatory parameters:** X-CM-WELL-TYPE and Content-Type HTTP headers, and the file to add.

----------

**Template:**

    curl -X POST <cm-well-path> -H "X-CM-WELL-TYPE: FILE" -H "Content-Type: <contentType>" --data-binary @<localFilePath>

**URL example:** N/A

**Curl example (REST API):**

    curl -X POST <cm-well-host>/example/files/f1.png -H "X-CM-WELL-TYPE: FILE" -H "Content-Type: image/png" --data-binary @image.file.png

## Parameters 1 ##

Parameter | Description | Values | Example | Reference
:----------|:-------------|:--------|:---------|:----------
X-CM-WELL-TYPE | HTTP header that indicates that this is a file infoton | Constant: "X-CM-WELL-TYPE: FILE" | N/A | N/A
Content-Type | HTTP header that indicates the file's MIME type. | See reference | "Content-Type: image/png" | [W3 Content-Type](https://www.w3.org/Protocols/rfc1341/4_Content-Type.html)

## Code Example 1 ##

### Call ###

    curl -X POST <cm-well-host>/example/files/f1.png -H "X-CM-WELL-TYPE: FILE" -H "Content-Type: image/png" --data-binary @image.file.png

### Results ###

    {"success":true}

## Syntax 2 ##

**URL:** <CMWellHost>/_in
**REST verb:** POST
**Mandatory parameters:** \<new file triple\>

----------

**Template:**

    curl -X POST <cm-well-host>/_in?format=<format> <new file triple> <optional field value triples>

**URL example:** N/A

**Curl example (REST API):**

    curl -X POST <cm-well-host>/_in?format=ntriples --data-binary @curlInput.txt

### File Contents ###
    <http://example/files/f3.txt> <cmwell://meta/sys#data> "plain text content" .
    <http://example/files/f3.txt> <cmwell://meta/sys#mimeType> "text/plain" .
    <http://example/files/f3.txt> <cmwell://meta/nn#someField> "some value" .

## Parameters 2 ##

Parameter | Description | Values | Example | Reference
:----------|:-------------|:--------|:---------|:----------
format | The format in which the triples are provided | json, jsonl, jsonld, n3, rdfxml, ntriples, nquads, turtle/ttl, yaml, trig, trix | format=n3 | [CM-Well Input and Output Formats](API.InputAndOutputFormats.md)

## Code Example 2 ##

### Call ###

    curl -X POST <cm-well-host>/_in?format=ntriples --data-binary @curlInput.txt

### File Contents ###
    <http://example/files/f2.png> <cmwell://meta/sys#base64-data> "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAYJJREFUeNqkUztOAzEUnGd7d8NKSNyAO1AgcQ/K1Ei5AAXKOUgfpaahRsoduAH0FBEfxWv7Mc+rIFAqEq/W9rM943ljW1QVxxSHI0tYLpf3bGcH4hch5zybTqcHoVer1cwIanD9uEETKMkJWv6N8vdAw35nsXNsXY0Bxe1VA8OGlBLMyNYpPAQ+K8TAogji4AgwjCIjWkc8tJSKMayzyortao56tiKCbfGIJNMkGOygSCZFMeTEuVG1YcMwDJWtJ7mjZBFbXeBlJK5AmzNQMI3UUlAxhv0h6NpcVXh+TbAUPBUx58ahqONo5hqPnj6lqPsE533BKXM4ablb23GhYELCvnP4ShFFRl8mXYAMsk9wJhET7tNLQTDp2sLs+WDONAaOa5xPiO/mg5nYjQQ7E5+eX7A1g+jYJ4c2RWBWKStLJjP3Qg94WFQBrG8u/pr48EqZTKHESFCxRIF6R4iQejw8Qe7MoO1+eWCXwYK3u8t/3ULD7C7SYj6fH/wW5Njn/C3AAGZG6t+dQqFoAAAAAElFTkSuQmCC" .
    <http://example/files/f2.png> <cmwell://meta/sys#mimeType> "image/png" .
    <http://example/files/f2.png> <cmwell://meta/nn#someField> "some value" .

### Results ###

    {"success":true}

## Notes ##

* When using syntax #1 (simple POST command), if you don't supply the Content-Type header, CM-Well will guess the file's MIME type. This is prone to error; therefore it's recommended to supply the Content-Type header.
* When supplying binary file contents within the command, encode them in base64.

## Related Topics ##
None.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.Purge.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.AddLinkInfoton.md)  

----