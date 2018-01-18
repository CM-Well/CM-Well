# Metadata Fields #

In addition to user-defined fields, infotons have fields created automatically by CM-Well. The following table summarizes these fields:

Metadata Type | Infoton Type | Metadata Fields
:-------------|:-------------|:----------------
system | All infotons |
content | File infotons | **data** - the file's textual data<br>**length** - the data size in bytes<br>**mimeType** - the file's Mime type
link | Link infotons   | **to** - the target infoton linked to<br>**kind** - kind of link (0=Permanent, 1=Temporary, 2=Forward)

parent
lastModified
path
uuid
quad
dataCenter
indexTime
current


Here is an example of an infoton with its **system** fields:

    {
      "type" : "ObjectInfoton",
      "system" : {
    	"uuid" : "64aba0e57e2670953024d59f3ecf275a",
    	"lastModified" : "2015-03-08T11:11:06.712Z",
    	"path" : "/permid.org/1-21525576839",
    	"dataCenter" : "dc1",
    	"indexTime" : 1425813066712,
    	"parent" : "/permid.org"
      },
      "fields" : {
    	"IsTradingIn.mdaas" : [ "EUR" ],
    	"QuoteExchangeCode.mdaas" : [ "FRA" ],
    	"CommonName.mdaas" : [ "UBS NCB PUT 14P" ],
    	"IsQuoteOf.mdaas" : [ "http://permid.org/1-21525557620" ],
    	"type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
    	"TRCSAssetClass.mdaas" : [ "Traditional Warrants" ],
    	"RCSAssetClass.mdaas" : [ "TRAD" ],
    	"ExchangeTicker.mdaas" : [ "UA44FB" ],
    	"RIC.mdaas" : [ "DEUA44FB.F^L14" ]
      }
    }

Using the **qp** operator, you can perform search queries in metadata fields as well as in user-defined fields. For example, the following query searches for file infotons that contain the string "configuration":
    
    <cm-well-host>/meta?qp=content.data:configuration

