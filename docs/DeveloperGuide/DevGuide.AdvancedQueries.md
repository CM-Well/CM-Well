# Advanced Queries

<a name="hdr1"></a>
## AND, OR and NOT Clauses
When querying for field values, you are not limited to one field.  You can query for infotons that match constraints on several field values.

Although CM-Well queries do not support operators named AND, OR and NOT, they do support different syntax for implementing the same functionality as these logical operators, as describes below.

!!! note
	You can group clauses in [ ] brackets, and apply * and - operators to the entire group. Brackets may also be nested.

### Field Lists as AND Clauses
To specify several field values as search filters, add all the name-value pairs to the query parameters, separating them with commas. 

For example, to find companies in Open PermID with addresses in Rochester, NY, you could use the following query:

```
    <cm-well-host>/permid.org/?op=search&qp=organizationCity.mdaas:Rochester,organizationStateProvince.mdaas:New%20York
```

This will return organizations that are located both in a city named Rochester, and in the state of New York.

Providing a comma-separated list of field value conditions has the same effect as putting an AND operator between conditions in Boolean logical syntax.

### Optional Fields for OR Clauses

CM-Well query syntax does not support an "OR" operator, but instead it supports the * operator to indicate that a condition is optional. This allows you to effectively implement an OR operation. Let's see how.

For example, to search for companies that have an address in Rochester, NY, and optionally have an address in Las Vegas, NV, you can use this query:

```
    <cm-well-host>/permid.org/?op=search&qp=[organizationCity.mdaas:Rochester,organizationStateProvince.mdaas:New%20York],*[organizationCity.mdaas:Las%20Vegas,organizationStateProvince.mdaas:Nevada]
```

!!! tip
	You can surround a list of conditions with square brackets **[**...**]**, so they can be defined as mandatory or optional as a group.

The query above has two groups of parameters, one for Rochester, the other for Las Vegas. The group for Las Vegas is preceded by the * symbol, to mark it as optional. The group for Rochester is mandatory.

This query returns all organizations that have an address in Rochester NY, and also those that have an address both in Rochester NY and in Las Vegas NV. This yields 2279 results. 

If we make the Rochester address optional, and Las Vegas mandatory, the query looks like this:

```
    <cm-well-host>/permid.org/?op=search
    &qp=*[organizationCity.mdaas:Rochester,organizationStateProvince.mdaas:New%20York],
    [organizationCity.mdaas:Las%20Vegas,organizationStateProvince.mdaas:Nevada]
```

Now the query returns all organizations that have an address in Las Vegas NV, and also those that have an address both in Rochester NY and in Las Vegas NV. This yields 8019 results. 

If we make both conditions optional, the query looks like this:

```
    <cm-well-host>/permid.org/?op=search
    &qp=*[organizationCity.mdaas:Rochester,organizationStateProvince.mdaas:New%20York],*[organizationCity.mdaas:Las%20Vegas,organizationStateProvince.mdaas:Nevada]
```

This query returns all organizations that have an address either in Las Vegas NV or in Rochester NY. This yields 10297 results. You may have noticed that the sum of the previous queries' results count is 2279 + 8019 = 10298, one more than 10297. This is because there is one organization that has an address both in Las Vegas NV and in Rochester NY, creating an overlap of 1 item in the previous queries' results.

!!! note
	Although we have described the \* operator as an "optional" operator, this is not strictly accurate. The query above will return any infoton that meets *one* of the criteria, but it must meet *at least* one criterion. Infotons that don't meet any of the criteria are not returned. Therefore when all clauses at the same nesting level are marked as optional, they perform just like an OR clause.

### "-" Operator for NOT Clauses
Preceding a field condition with the - character creates a NOT condition; that is, results that meet the condition are filtered out.

For example, the following query returns all organizations that have an address in New York state, but *not* in the city of Rochester:

```
    <cm-well-host>/permid.org?op=search&qp=organizationStateProvince.mdaas:New%20York,-[organizationCity.mdaas:Rochester]
```

The - operator can be also used to test for the non existence of a field. For example, the following query returns all organizations that have a province of New York but no city defined:

```
    <cm-well-host>/permid.org?op=search&qp=organizationStateProvince.mdaas:New%20York,-organizationCity.mdaas:
```
 
<a name="hdr2"></a>
## String-Matching Logic

### Partial Matches
All of the queries we’ve defined up to this point return results whose field values contain the query value as a sub-string. These are know as **partial matches**.

For example, let's try searching for the name string "Thomson Corp" as follows:

```
    <cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Thomson%20Corp,type.rdf:Organization
```

This returns several results whose CommonName field contains "Thomson Corp" as a sub-string. This includes "Thomson Corp", "Thomson Corp Delaware, Inc.", "Thomson Corp Medical Education", "Standard Thomson Corp" and so on.

### Exact Matches

Suppose you're only interested in retrieving the Thomson Corp parent company, or in other words, you want to perform an **exact match** on the "Thomson Corp" value.  To do this, change the single colon '**:**' in the query parameter to a double colon '**::**' as follows:

```
    <cm-well-host>/permid.org?op=search&qp=CommonName.mdaas::Thomson%20Corp,type.rdf:Organization&with-data
```

The query returns only one result, with the CommonName value of “Thomson Corp”.  

!!! note
	Be careful when using exact matches. if the company’s common name was listed as “The Thomson Corp”, then your exact-match query would not have worked! 

### Fuzzy Matches
Suppose you’re not entire sure how "Thomson" is spelled.  For instance, it could be spelled "Thompson". You can choose to cast a wider net over the data by performing a **fuzzy match** search. 

To perform a fuzzy match search, replace the colon ':' with a tilde '~', as follows:

```
    <cm-well-host>/permid.org?op=search&qp=CommonName.mdaas~Thomson%20Corp,type.rdf:Organization&with-data
```

This returns results that are somewhat similar to your search string. For instance, the fuzzy-matched values could have additional characters to the query value, or less characters, or a few changed characters.

!!! warning
	Be advised that the number of responses returned for a fuzzy match is much larger than for an exact or partial match, since it the results can contain anything remotely matching your search string. Also, note that the order of the results is not necessarily guaranteed to be “strongest match first”. When using a fuzzy match, it's recommended to either include other criteria in your search in order to narrow down the results set, or be prepared to download a large set of answers and handle the ranking in your application.

<a name="hdr3"></a>
## Returning a Subset of Fields

By default, when you run CM-Well queries using the **with-data** flag, you get the entire infoton in the results. For larger result sets, this can mean that your application will have a large amount of data to process. This is a potential waste of bandwidth, particularly if all you really wanted was one or two fields from each infoton.

To narrow down the result set, you can specify a filter that lists the fields you want to receive in the query results. You do this by using the **fields** parameter.

For example, let’s query for Thomson Reuters in Open Permid data, but specifiy that we only want to receive the name of the company, and the year it was founded. The query would look like this:

```
    <cm-well-host>/permid.org?op=search&qp=CommonName.mdaas::Thomson%20Corp,type.rdf:Organization&fields=CommonName.mdaas,organizationFoundedYear.mdaas&with-data 
```

The results would then be:

```
    @prefix mdaas: <http://ont.thomsonreuters.com/mdaas/> .
    @prefix sys:   <http://cm-well-ppe.int.thomsonreuters.com/meta/sys#> .
    @prefix nn:<http://cm-well-ppe.int.thomsonreuters.com/meta/nn#> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix o: <http://permid.org/> .
    
    { 
	  	o:1-5044334019  sys:dataCenter "na" ;
      	sys:indexTime  "1444833797895"^^xsd:long ;
      	sys:lastModified   "2015-09-21T21:01:48.541Z"^^xsd:dateTime ;
      	sys:parent "/permid.org" ;
      	sys:path   "/permid.org/1-5044334019" ;
      	sys:type   "$anon$1" ;
      	sys:uuid   "078e1d7f7caf6c376eec5437c539ce2c" ;
      	mdaas:CommonName   "Thomson Corp" ;
      	mdaas:organizationFoundedYear  "1972" .
    }
```


!!! note
	System fields are always returned regardless of the **fields** filter, but the only object fields returned are those we requested.

<a name="hdr4"></a>
## Searching by Value Ranges
So far our query examples have used field values of the string type. But CM-Well also supports number and date values.  

Suppose we want to search for all the organizations that were founded after the year 2014. We could use this query:

```
    <cm-well-host>/permid.org?op=search&qp=organizationFoundedYear.mdaas>2014,type.rdf:Organization&with-data&format=ttl
```

!!! note
	Note the use of the > operator, which relates to the year as a numeric value. You can use 4 comparators on numeric field values: >, <, >>, <<. (See table below.)

The various comparators that you can use in queries are:

Comparator | Type
:----------|:------
:	| Partial match
::	| Exact match
~	| Fuzzy match
\<	| Less than
\>	| Greater than
\<<	| Less than or equal to
\>>	| Greater than or equal to

<a name="hdr5"></a>
## Retrieving Historic Versions

When an infoton is updated in CM-Well, the older version is saved as is, and a new, updated version is created. The entity's CM-Well URI (for instance, the URI containing  the permID, for an organization or person) always points to the most recent version.

You can choose to retrieve all historical versions of an infoton. To do this, include the **with-history** flag in the query.

For example:

```
    <cm-well-host>/data.com/2-a6165f1cadc6b7d592e27384ca5d58733b7a4a249b1d4bd935800ebfa59fae63?with-history&format=ttl 
```
    
This query returns at least two versions of the entity's infoton. 

!!! note
	The order in which historical versions are returned is unpredictable, so you’ll likely want to rank the results yourself in your application, using the sys:lastModified attribute.
