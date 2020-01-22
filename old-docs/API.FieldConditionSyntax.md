# Field Condition Syntax #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.QueryParameters.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.FieldNameFormats.md)  

----

The following sections describe how to create simple and complex conditions on infoton field values. The conditions appear in the value of the **qp** parameter, as follows:

    <cmwellhost>/<path>?op=search&qp=<field conditions>

[Single Condition Syntax](#hdr1) 

[Comparison Operators](#hdr2)
 
[Field Values](#hdr3) 

[Multiple Conditions ("AND")](#hdr4) 

[Optional Conditions ("OR")](#hdr5) 

[Negative Conditions ("NOT")](#hdr6) 

[Grouped Conditions](#hdr7) 

[Field Identifiers](#hdr8) 

[Data Fields and Metadata (System) Fields](#hdr9) 

<a name="hdr1"></a>
## Single Condition Syntax ##

A single condition's syntax is:

    (FieldOperator) FieldName ComparisonOperator FieldValue

These are the components of the condition:

1. **FieldOperator** - An optional field operator (- to indicate [negation](#hdr6) or * to indicate an [optional condition](#hdr5)). If no field operator is used, the field condition must apply to all query results.
2. **FieldName** - The [name of the field](#hdr8) on which you want to apply the condition.
3. **ComparisonOperator** - The [comparison operator](#hdr2).
4. **FieldValue** - The [field value](#hdr3).

Here are some examples of field conditions:

Example | Description
:--------|:-------------
`-TRCSAssetClass.mdaas:Ordinary` | TRCSAssetClass must *not* equal "Ordinary"
`CommonName.mdaas::Intel` | CommonName must be an exact match of "Intel"
`dsQuotationNumber.mdaas>290670` | dsQuotationNumber must be greater than 290670

<a name="hdr2"></a>    
## Comparison Operators ##

The following table describes the different comparison operators you can apply to field values.

Operator | Apply to: | Description | Example
:---------:|:-----------|:-------------|:--------
**:** | Any literal type | Partial match. True if the value operand is contained in the field value. This operator is case-insensitive. | `CommonName.mdaas:new` matches both "New Balance" and "New Generation" CommonName values.
**::** | string | Exact match. True if the value operand equals the field value. This operator is case-sensitive. | `CommonName.mdaas::Intel`
**~** | string | Fuzzy match. True if the value operand is an approximate partial match of the field value. Fuzzy match ignores a certain amount of extra, missing or transposed characters. | `CommonName.mdaas~converse` matches "Tae Gwang Commerce", "Rydex Inverse NASDAQ-100 Strategy Fund", "Cloverie PLC Series 2009-01"...
**\>** | Number and date values  | Greater than\* | instrumentExpiryDate.mdaas>2015-01-01
**\>\>** | Number and date values  | Equal or greater than\* | organizationTier.fedapioa>>2
**\<** | Number and date values  | Less than* | modifiedByAnalystId.fedapioa<100
**\<\<** | Number and date values  | Equal or less than\* | modifiedByAnalystId.fedapioa<100

> \* Comparison order depends on the field's type, as defined in RDF:
> * Numeric for number values
> * Lexicographic for strings
> * Chronological for date/datetime values

----------


>**Note:** Fuzzy matches may return a very large number of results. It is recommended to limit fuzzy match queries with other constraints and/or the **length** parameter. 

<a name="hdr3"></a>
## Field Values ##

Field values are alphanumeric strings. Do not use surrounding quotation marks. 

**Field value examples:**

`Coca%20Cola`, `12345`, `23andme`

Certain characters require special handling.

As is standard for URLs, all characters except for `A–Z`, `a–z`, `0–9`, `*`, `-`, `.` and `_` must be encoded as %HH (their hexadecimal representation). For example, spaces are encoded as %20, `#` characters are encoded as %23, and so on.

Characters that have a special meaning in CM-Well query syntax must be escaped in a different way. These include the characters: `:`, `<`, `>`, `$`, `,` and `]`.
To include one of these characters in a field value, escape the entire field value by surrounding it with `$` characters on each side.

For example:  `width:length` should be escaped as `$width:length$`.

To include the `$` character, double the character in the field value, to differentiate it from the escaping characters.

For example:  `parent$child` should be escaped as `$parent$$child$`.

<a name="hdr4"></a>
## Multiple Conditions ("AND") ##
To apply multiple field conditions, add several condition to the query, separated by commas. Infotons that match the query must fulfill all the conditions in the list. The comma is the equivalent of the Boolean AND operator.

**Example:**

This query returns organization infotons with a city value of "Rochester" and a state value of "New York".

    <cm-well-host>/permid.org/?op=search&qp=organizationCity.mdaas:Rochester,organizationStateProvince.mdaas:New%20York

<a name="hdr5"></a>
## Optional Conditions ("OR") ##

CM-Well query syntax does not support an "OR" operator, but instead it supports the * operator to indicate that a condition is optional. This allows you to effectively implement an OR operation.

For example, to search for organizations that have an address in New York state, and *optionally* have an address in Nevada, you can use this query:


    <cm-well-host>/permid.org?op=search&qp=organizationStateProvince.mdaas:New%20York,*organizationStateProvince.mdaas:Nevada

If you make *both* state conditions optional, the query will return organizations that have an address either in New York state or in Nevada:

    <cm-well-host>/permid.org?op=search&qp=*organizationStateProvince.mdaas:New%20York,*organizationStateProvince.mdaas:Nevada

The * operator does not precisely translate to "optional", as the query above means that *at least one* of the conditions must be met. Thus, the effect is the same as putting a Boolean OR operator between the two conditions.

<a name="hdr6"></a>
## Negative Conditions ("NOT") ##

To specify that a certain condition *should not* be met, you can precede the condition with the - operator. This is equivalent to preceding a condition with the Boolean NOT operator.

For example, to retrieve all organizations that *do not* have an address in New York state, you could run the following query:

    <cm-well-host>/permid.org?op=search&qp=-organizationStateProvince.mdaas:New%20York

<a name="hdr7"></a>
## Grouped Conditions ##

You may want to apply a - or * operator to several conditions. To do this, surround the list of conditions with [ ] brackets, and apply the operator before the brackets.

For example, to retrieve all organizations whose address is neither in New York state nor in California, you could run the following query:

    <cm-well-host>/permid.org?op=search&qp=-[organizationStateProvince.mdaas:New%20York,organizationStateProvince.mdaas:California]

<a name="hdr8"></a>
## Field Identifiers ##
See [Field Name Formats](API.FieldNameFormats.md).

<a name="hdr9"></a>
## Data Fields and Metadata (System) Fields ##

When infotons are created in CM-Well, they are usually created together with data fields, which are uploaded in the form of triples related to the infoton. The data fields are often derived from another data structure, such as CommonName fields that come from Organization Authority.

CM-Well also maintains its own internal administrative details about each infoton, and it stores these details in system metadata fields. These are items such as the datetime the infoton was last modified, or the internal UUID for the infoton.

When you query for infotons, both data fields and system metadata fields are returned in the results. You can apply conditions to the metadata fields as well as the data fields (see [Field Identifiers](#hdr8)). 


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.QueryParameters.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.FieldNameFormats.md)  

----