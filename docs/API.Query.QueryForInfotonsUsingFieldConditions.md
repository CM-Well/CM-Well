# Function: *Query for Infotons Using Field Conditions* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Get.GetSingleInfotonByUUID.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Query.ApplySPARQLToQueryResults.md)  

----

## Description ##
This function enables you to search for infotons whose field values meet specific conditions. You can define one or more conditions on field values, and you can specify different types of comparison operators (e.g. exact or partial string match, numeric inequality...). The search returns only those infotons that reside under the path you specify, which meet the field value conditions.

For example, you can search for all organizations under the permid.org branch of CM-Well which are located in New York state, by defining a query with the permid.org path and a condition on the value of the organizationStateProvince field.

>**Note:** By default, search queries are not recursive. So, for example, if you supply a search path of \<CMWellHost\>/\<SomePath\>, the query returns only infotons that are directly under SomePath. If they have other descendant infotons, these are not returned unless explicitly requested. To make a search operation recursive, use the **recursive** flag, as follows:

>`<cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Coca%20Cola&length=1&format=n3&recursive`

## Syntax 1 ##

**URL:** ```<hostURL>/<PATH>```
**REST verb:** GET
**Mandatory parameters:** ```op=search&qp=<fieldConditions>```

----------

**Template:**

    <cm-well-path>?op=search&qp=<fieldConditions>&<otherParameters...>

**URL example:**
   `<cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Coca%20Cola&length=1&format=n3&with-data`

**Curl example (REST API):**

    Curl -X GET <cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Coca%20Cola&length=1&format=n3&with-data

## Code Example ##

### Call ###

    <cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Coca%20Cola&length=1&format=n3&with-data

### Results ###

    @prefix nn:<cm-well-host/meta/nn#> .
    @prefix mdaas: <http://ont.com/mdaas/> .
    @prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix sys:   <cm-well-host/meta/sys#> .
    
    <http://permid.org/1-21588085919>
    amdaas:Quote ;
    sys:dataCenter   "dc1" ;
    sys:indexTime"1463590815956"^^xsd:long ;
    sys:lastModified "2016-05-18T17:00:14.565Z"^^xsd:dateTime ;
    sys:parent   "/permid.org" ;
    sys:path "/permid.org/1-21588085919" ;
    sys:type "ObjectInfoton" ;
    sys:uuid "4342745f947e9abb5c32367dd0e4689f" ;
    mdaas:CommonName "COCA-COLA AM SSBA 31DEC2029 7.65CWNT" ;
    mdaas:ExchangeTicker "CCLKOA" ;
    mdaas:IsTradingIn"AUD" ;
    mdaas:QuoteExchangeCode  "ASX" ;
    mdaas:RCSAssetClass  "TRAD" ;
    mdaas:RIC"CCLKOAta.AX" ;
    mdaas:TRCSAssetClass "Traditional Warrants" .
    
    [ sys:pagination  [ sys:first  <cm-well-host/permid.org?format=n3?&op=search&from=2016-05-18T17%3A00%3A14.565Z&to=2016-05-18T17%3A00%3A14.565Z&qp=CommonName.mdaas%3ACoca+Cola&length=1&offset=0> ;
    sys:last   <cm-well-host/permid.org?format=n3?&op=search&from=2016-05-18T17%3A00%3A14.565Z&to=2016-05-18T17%3A00%3A14.565Z&qp=CommonName.mdaas%3ACoca+Cola&length=1&offset=11605> ;
    sys:next   <cm-well-host/permid.org?format=n3?&op=search&from=2016-05-18T17%3A00%3A14.565Z&to=2016-05-18T17%3A00%3A14.565Z&qp=CommonName.mdaas%3ACoca+Cola&length=1&offset=1> ;
    sys:self   <cm-well-host/permid.org?format=n3?&op=search&from=2016-05-18T17%3A00%3A14.565Z&to=2016-05-18T17%3A00%3A14.565Z&qp=CommonName.mdaas%3ACoca+Cola&length=1&offset=0> ;
    sys:type   "PaginationInfo"
      ] ;
      sys:results [ sys:fromDate  "2016-05-18T17:00:14.565Z"^^xsd:dateTime ;
    sys:infotons  <http://permid.org/1-21588085919> ;
    sys:length"1"^^xsd:long ;
    sys:offset"0"^^xsd:long ;
    sys:toDate"2016-05-18T17:00:14.565Z"^^xsd:dateTime ;
    sys:total "11605"^^xsd:long ;
    sys:type  "SearchResults"
      ] ;
      sys:type"SearchResponse"
    ] .

## Syntax 2 ##

For some complex queries, the length of the field conditions might exceed the limitation that some clients impose on GET commands. In this case, you can use an alternate POST syntax, while providing the query parameters in the request's body rather than in its URL, as in the example below.

If you use this syntax, please pay attention to the following:

>**NOTES:** 
>* ```op=search``` is a mandatory query parameter and cannot be moved to the request body. All other parameters, including ```qp```, can be moved to the request body.
>* The path value still determines which CM-Well directory will be searched.
>* For this syntax, the content type should be ```application/x-www-form-urlencoded```.


**URL:** ```<hostURL>/<PATH>```
**REST verb:** POST
**Mandatory parameters:** ```op=search```

----------

**Curl example (REST API):**

```curl –X POST "cmwell/example.org?op=search" -H Content-Type:application/x-www-form-urlencoded --data-binary 'recursive=true&qp=FN.vcard::Jane%20Smith&format=tsv'```

### Results ###

```/example.org/JaneSmith     2018-07-29T08:47:15.665Z   844a646f09b1e39b1be144e8d4f4fbbf       1532854035927```

## Notes ##

* If you don’t know the precise field name to search in, you can indicate that you want to search in all fields, as follows:  *qp=_all:Marriott%20Ownership%20Resorts*. Use this option with caution as it may be expensive in terms of run-time.
* Special characters must be escaped in the usual way for URIs. For example, spaces are escaped via %20, # characters are encoded using %23, and so on.
* Although most RDF structures use the standard of *prefix:fieldname*, in CM-Well you add the field name prefix *after* the name itself, as follows: *fieldname.prefix*. For example "CommonName.mdaas".
* You can surround a list of conditions with square brackets [...], so they can be defined as mandatory or optional as a group.
* Note that using fuzzy matching may produce a very large number of results. When using fuzzy match, it's recommended to include other criteria in your search in order to narrow down the results set.

## Related Topics ##
[Basic Queries](DevGuide.BasicQueries.md)
[Advanced Queries](DevGuide.AdvancedQueries.md)
[Applying SPARQL to Query Results](API.Query.ApplySPARQLToQueryResults.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Get.GetSingleInfotonByUUID.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Query.ApplySPARQLToQueryResults.md)  

----