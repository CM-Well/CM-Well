# Function: *Get Single Infoton by UUID* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Get.GetMultipleInfotonsByURI.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Query.QueryForInfotonsUsingFieldConditions.md)  

----

## Description ##

Every infoton has a UUID (UUID = Unique Universal IDentifier) field and value, in addition to having a unique URL. The UUID is used to differentiate among different versions of the same infoton. As CM-Well maintains an immutable-data principle, every time an infoton is updated, a new version of it is written, with a new UUID.

You can retrieve all historical versions of an infoton by performing a GET action on it while using the **with-history** flag. Once you have the list of versions, you can retrieve each version using a special path composed of the **ii** endpoint and the UUID value.

## Syntax ##

**URL:** \<CMWellHost\>/ii
**REST verb:** GET
**Mandatory parameters:** N/A

----------

**Template:**

    <CMWellHost>/ii/<InfotonUUID>

**URL example:**
   `<cm-well-host>/ii/c6d3d9acf8c174b9d1fdc8d16fc43fa6`

**Curl example (REST API):**

    Curl -X GET <cm-well-host>/ii/c6d3d9acf8c174b9d1fdc8d16fc43fa6

## Code Example ##

### Call ###

    <cm-well-host>/ii/c6d3d9acf8c174b9d1fdc8d16fc43fa6?format=ttl

### Results ###

    @prefix nn:    <cm-well-host/meta/nn#> .
    @prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix sys:   <cm-well-host/meta/sys#> .
    @prefix prov:  <http://www.w3.org/ns/prov#> .
    @prefix o: <http://data.com/> .
    
    o:2-38caa59fd622a62c5449d7d76b91dee7c46827b9eb936abf0ec299f3b7dbe544
    a   <http://metadata.com/IngestActivity> ;
    sys:dataCenter          "dc1" ;
    sys:indexTime           "1472776555879"^^xsd:long ;
    sys:lastModified        "2016-09-02T00:35:55.040Z"^^xsd:dateTime ;
    sys:parent              "/data.com" ;
    sys:path                "/data.com/2-38caa59fd622a62c5449d7d76b91dee7c46827b9eb936abf0ec299f3b7dbe544" ;
    sys:type                "ObjectInfoton" ;
    sys:uuid                "c6d3d9acf8c174b9d1fdc8d16fc43fa6" ;
    prov:endedAt            "2016-09-01T19:33:52Z"^^xsd:dateTime ;
    prov:startedAt          "2016-09-01T19:33:52Z"^^xsd:dateTime ;
    prov:wasAssociatedWith  <urn:com.etl.ld:cmp-ingestor:unspecified.7831935:2016-06-24T10:33:43-0500:jenkins-cmp-full-ingest-180:180:2016-06-24_10-33-22:shadowJar> .

## Notes ##
None.

## Related Topics ##
[Get Single Infoton by URI](API.Get.GetSingleInfotonByURI.md)
[Using the with-history Flag](API.UsingTheWith-historyFlag.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Get.GetMultipleInfotonsByURI.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Query.QueryForInfotonsUsingFieldConditions.md)  

----