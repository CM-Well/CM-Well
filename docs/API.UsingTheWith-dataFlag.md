# Using the with-data Flag #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.UsingTheRecursiveFlag.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.UsingTheWith-historyFlag.md)  

----

CM-Well infotons have two types of fields:

* System metadata fields that contain internal administrative details filled in by CM-Well.
* The infoton's own fields, as defined by the data model the infoton is implementing. These field values are populated by triples added to the infoton.

CM-Well queries by default return infotons "without data", that is, only the infoton's URL and its system metadata fields are returned.

If the **with-data** flag is used, all of the infoton's fields are returned in the query results.

The **with-data** flag can be added with no value, in which case the data is returned in N3 format. You can also add a format value to **with-data**, that can be any format that CM-Well supports, for instance: with-data=yaml, with-data=json, with-data=trig...

>**Note:** The with-data flag can only be used with the **search** API (not with other read functions such as retrieval by URI or calling the _out endpoint).

For example, here are some query results (truncated) without data:

    @prefix nn:<cm-well-host/meta/nn#> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix sys:   <cm-well-host/meta/sys#> .
    
    <http://permid.org/1-21588400951>
    sys:dataCenter"dc1" ;
    sys:indexTime "1464380785276"^^xsd:long ;
    sys:lastModified  "2016-05-27T20:26:23.613Z"^^xsd:dateTime ;
    sys:parent"/permid.org" ;
    sys:path  "/permid.org/1-21588400951" ;
    sys:type  "ObjectInfoton" ;
    sys:uuid  "0fd092a7ec71351301681a67ec851953" .
    
    <http://permid.org/1-4295903091>
    sys:dataCenter"dc1" ;
    sys:indexTime "1464381251635"^^xsd:long ;
    sys:lastModified  "2016-05-27T20:34:10.427Z"^^xsd:dateTime ;
    sys:parent"/permid.org" ;
    sys:path  "/permid.org/1-4295903091" ;
    sys:type  "ObjectInfoton" ;
    sys:uuid  "bb657a3f2df24a100ead15920ec565ad" .
    
    <http://permid.org/1-21588085388>
    sys:dataCenter"dc1" ;
    sys:indexTime "1464380869582"^^xsd:long ;
    sys:lastModified  "2016-05-27T20:27:48.879Z"^^xsd:dateTime ;
    sys:parent"/permid.org" ;
    sys:path  "/permid.org/1-21588085388" ;
    sys:type  "ObjectInfoton" ;
    sys:uuid  "6cade86aeeac81d6cea957b87e059e05" .
    
    <http://permid.org/1-21588085918>
    sys:dataCenter"dc1" ;
    sys:indexTime "1464380923253"^^xsd:long ;
    sys:lastModified  "2016-05-27T20:28:42.220Z"^^xsd:dateTime ;
    sys:parent"/permid.org" ;
    sys:path  "/permid.org/1-21588085918" ;
    sys:type  "ObjectInfoton" ;
    sys:uuid  "d5b36e0296e7c30153266d99a14682a8" .
    ...


And here are the same results (truncated) with data:

    @prefix nn:<cm-well-host/meta/nn#> .
    @prefix mdaas: <http://ont.com/mdaas/> .
    @prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix sys:   <cm-well-host/meta/sys#> .
    
    <http://permid.org/1-21588400951>
    amdaas:Quote ;
    sys:dataCenter   "dc1" ;
    sys:indexTime"1464380785276"^^xsd:long ;
    sys:lastModified "2016-05-27T20:26:23.613Z"^^xsd:dateTime ;
    sys:parent   "/permid.org" ;
    sys:path "/permid.org/1-21588400951" ;
    sys:type "ObjectInfoton" ;
    sys:uuid "0fd092a7ec71351301681a67ec851953" ;
    mdaas:CommonName "Coca-Cola 18 NOV" ;
    mdaas:ExchangeTicker "0V8" ;
    mdaas:QuoteExchangeCode  "IEU" ;
    mdaas:RCSAssetClass  "FUT" ;
    mdaas:RIC"CCHLFDCX1816" ;
    mdaas:TRCSAssetClass "Equity Futures" .
    
    <http://permid.org/1-4295903091>
    a   mdaas:Organization ;
    sys:dataCenter  "dc1" ;
    sys:indexTime   "1464381251635"^^xsd:long ;
    sys:lastModified"2016-05-27T20:34:10.427Z"^^xsd:dateTime ;
    sys:parent  "/permid.org" ;
    sys:path"/permid.org/1-4295903091" ;
    sys:type"ObjectInfoton" ;
    sys:uuid"bb657a3f2df24a100ead15920ec565ad" ;
    mdaas:CIK   "0000021344" ;
    mdaas:CommonName"Coca-Cola Co" ;
    mdaas:InvestextID   "KO" ;
    mdaas:LEI   "UWJKFUJFZ02DKWI3RY53" ;
    mdaas:MXID  "100085264" ;
    mdaas:OrganizationProviderTypeCode
    "1" ;
    mdaas:PrimaryReportingEntityCode
    "1996N" ;
    mdaas:RCPID "300012286" ;
    mdaas:SDCID "1329001 " ;
    mdaas:SDCusip   "191216" ;
    mdaas:TaxID "580628465" ;
    mdaas:VentureEconomicsID"25836" ;
    mdaas:WorldscopeCompanyID   "191216100" ;
    mdaas:WorldscopeCompanyPermID   "C840L6930" ;
    mdaas:akaName   "Coke" , "Coca-Cola Co" ;
    mdaas:entityLastReviewedDate"2016-03-02 05:00:00"^^xsd:dateTime ;
    mdaas:equityInstrumentCount "9" ;
    mdaas:hasImmediateParent<http://permid.org/1-4295903091> ;
    mdaas:hasRegistrationAuthority  <http://permid.org/1-5000008957> ;
    mdaas:hasUltimateParent <http://permid.org/1-4295903091> ;
    mdaas:headquartersAddress   "1 Coca Cola Plz NW\nATLANTA\nGEORGIA\n30313-2420\nUnited States\n" , "ATLANTA\nGEORGIA\n30313-2420\nUnited States\n"@en ;
    mdaas:headquartersCommonAddress
    "1 Coca Cola Plz NW\nATLANTA\nGEORGIA\n30313-2420\nUnited States\n" ;
    mdaas:headquartersFax   "14046766792" ;
    mdaas:headquartersPhone "14046762121" ;
    mdaas:instrumentCount   "1776" ;
    mdaas:ipoDate   "1950-01-26 05:00:00"^^xsd:dateTime ;
    mdaas:isDomiciledIn "United States"@en ;
    mdaas:isIncorporatedIn  "United States"@en ;
    mdaas:isPublicFlag  "true" ;
    ...

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.UsingTheRecursiveFlag.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.UsingTheWith-historyFlag.md)  

----