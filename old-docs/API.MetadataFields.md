# Metadata Fields #

In addition to user-defined fields, infotons have fields that are created automatically by CM-Well. The following table summarizes these fields and their types:

Metadata Type | Infoton Type | Metadata Fields
:-------------|:-------------|:----------------
system | All infotons | **parent** - the immediate parent infoton of the given infoton<br>**parent.parent_hierarchy** - the entire "ancestor" hierarchy of the given infoton, in ascending order of hierarchy<br>**lastModified** - the time the infoton was last modified<br>**path** - the infoton's path in CM-Well.<br>*NOTE: The path value does not contain the protocol value (**http** or **https**); which is stored in the **protocol** system field - see below.*<br>**uuid** - the infoton's unique ID<br>**quad** - all labels of the named graphs with which the infoton has an associated statement <br>**dataCenter** - the data center where the original version of the infoton was first written<br>**indexTime** - the time the infoton was indexed<br>**current** - whether the infoton is the current version or a historic version<br>**protocol** - the protocol (**http** or **https**) provided when the infoton was last ingested. If no protocol value is provided, the default is **http**. See [The Protocol System Field](API.ProtocolSystemField.md) to learn more.
content | File infotons | **data** - the file's textual data<br>**length** - the data size in bytes<br>**mimeType** - the file's Mime type
link | Link infotons   | **to** - the target infoton linked to<br>**kind** - kind of link (0=Permanent, 1=Temporary, 2=Forward)

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

Using the **qp** operator, you can perform search queries in metadata fields as well as in user-defined fields. For example, the following query searches for file infotons whose text body contains the string "configuration":
    
    <cm-well-host>/meta?qp=content.data:configuration

