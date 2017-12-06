# Function: *Get Single Infoton by URI* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Login.Login.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Get.GetMultipleInfotonsByURI.md)  

----

## Description ##
If you have an infoton's CM-Well URI (the path in CM-Well pointing to the infoton), you can perform a simple GET command to retrieve all of the infoton's field names and values.

## Syntax ##

**URL:** Infoton's URI.
**REST verb:** GET
**Mandatory parameters:** N/A

----------

**Template:**

    <CMWellHost>/<InfotonPath>

**URL example:**
   `<cm-well-host>/permid.org/1-5050986961`

**Curl example (REST API):**

    Curl -X GET <cm-well-host>/permid.org/1-5050986961

## Code Example ##

### Call ###

    <cm-well-host>/permid.org/1-5050986961?format=ttl

### Results ###

    @prefix nn:<cm-well-host/meta/nn#> .
    @prefix mdaas: <http://ont.com/mdaas/> .
    @prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix sys:   <cm-well-host/meta/sys#> .
    @prefix o: <http://permid.org/> .
    
    o:1-5050986961  a   mdaas:Organization ;
        sys:dataCenter                  "dc1" ;
        sys:indexTime                   "1463668800037"^^xsd:long ;
        sys:lastModified                "2016-05-19T14:39:59.335Z"^^xsd:dateTime;
        sys:parent                      "/permid.org" ;
        sys:path                        "/permid.org/1-5050986961" ;
        sys:type                        "ObjectInfoton" ;
        sys:uuid                        "5c7293a6d8af048efd38168a5f624fbd" ;
        mdaas:CommonName                "URSUS Medical LLC" ;
        mdaas:OrganizationProviderTypeCode "1" ;
        mdaas:equityInstrumentCount     "0" ;
        mdaas:hasRegistrationAuthority  o:1-5000007206 ;
        mdaas:instrumentCount           "0" ;
        mdaas:isIncorporatedIn          "United States"@en ;
        mdaas:isPublicFlag              "false" ;
        mdaas:officialName              "URSUS Medical LLC" ;
        mdaas:organizationFoundedDay    "21" ;
        mdaas:organizationFoundedMonth  "4" ;
        mdaas:organizationFoundedYear   "2014" ;
        mdaas:organizationStatusCode    "Active" ;
        mdaas:organizationSubtypeCode   "Company" ;
        mdaas:organizationTypeCode      "Business Organization" ;
        mdaas:shortName                 "URSUS Medical" ;
        mdaas:subsidiariesCount         "0" .


## Notes ##

* Paste an infoton's URI in a browser's address bar to see an HTML presentation of the infoton's fields.
* Each time an infoton is updated, an additional "historical" version of the infoton is saved, and the infoton's URL will point to the newest version. 

## Related Topics ##
[Basic Queries](DevGuide.BasicQueries.md)
[Get Multiple Infotons by URI](API.Get.GetMultipleInfotonsByURI.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Login.Login.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Get.GetMultipleInfotonsByURI.md)  

----