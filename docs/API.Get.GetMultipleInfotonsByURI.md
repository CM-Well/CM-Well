# Function: *Get Multiple Infotons by URI* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Get.GetSingleInfotonByURI.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Get.GetSingleInfotonByUUID.md)  

----

## Description ##
Retrieve multiple infotons, by URI, in the same query.

## Syntax ##

**URL:** \<hostURL\>/_out
**REST verb:** POST
**Mandatory parameters:** Data containing paths of infotons to retrieve.

----------

**Template:**

    <CMWellHost>/_out?format=<format> -d <infoton path data>

**URL example:** N/A

**Curl examples (REST API):**

Plain text input:

    curl -X POST <cm-well-host>/_out?format=ntriples -H "Content-Type: text/plain" -d "/example.org/JohnSmith"
	
JSON input:

    curl -X POST <cm-well-host>/_out?format=ntriples -H "Content-Type: application/json" -d ' { "type":"InfotonPaths", "paths":[ "/example.org/JohnSmith", "/example.org/JaneSmith" ] }'

## Code Example ##

### Call ###

    curl -X POST <cm-well-host>/_out?format=ntriples -H "Content-Type: text/plain" --data-binary @curlInput.txt

### File Contents ###

    /example.org/JohnSmith
    /example.org/JaneSmith
    
### Results ###

    _:BretrievablePaths <cm-well-host/meta/sys#infotons> <http://example.org/Individuals/JohnSmith> .
    _:BretrievablePaths <cm-well-host/meta/sys#irretrievablePaths> "/example.org/Individuals/JaneSmith" .
    _:BretrievablePaths <cm-well-host/meta/sys#size> "1"^^<http://www.w3.org/2001/XMLSchema#int> .
    _:BretrievablePaths <cm-well-host/meta/sys#type> "RetrievablePaths" .
    <http://example.org/Individuals/JohnSmith> <http://purl.org/vocab/relationship/friendOf> <http://example.org/Individuals/PeterParker> .
    <http://example.org/Individuals/JohnSmith> <cm-well-host/meta/sys#dataCenter> "dc1" .
    <http://example.org/Individuals/JohnSmith> <http://www.lbd.com/bold#active> "true" .
    <http://example.org/Individuals/JohnSmith> <cm-well-host/meta/sys#uuid> "aa4726dea9981964553c79b12d643274" .
    <http://example.org/Individuals/JohnSmith> <cm-well-host/meta/sys#indexTime> "1460043319486"^^<http://www.w3.org/2001/XMLSchema#long> .
    <http://example.org/Individuals/JohnSmith> <cm-well-host/meta/sys#parent> "/example.org/Individuals" .
    <http://example.org/Individuals/JohnSmith> <cm-well-host/meta/sys#path> "/example.org/Individuals/JohnSmith" .
    <http://example.org/Individuals/JohnSmith> <cm-well-host/meta/sys#type> "ObjectInfoton" .
    <http://example.org/Individuals/JohnSmith> <cm-well-host/meta/sys#lastModified> "2016-04-07T15:35:18.091Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
    <http://example.org/Individuals/JohnSmith> <http://purl.org/vocab/relationship/parentOf> <http://example.org/Individuals/SaraSmith> .

## Notes ##
Use the PUT verb as well as the POST verb when directing a call to the _out endpoint.

## Related Topics ##
[Retrieve Single Infoton by URI](API.Get.GetSingleInfotonByURI.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Get.GetSingleInfotonByURI.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Get.GetSingleInfotonByUUID.md)  

----