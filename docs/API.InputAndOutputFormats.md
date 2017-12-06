# CM-Well Input and Output Formats	 #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.QueryParameters.md)  

----

There are several formats for encoding RDF triples (see [CM-Well Data Paradigms](Intro.CM-WellDataParadigms.md) to learn more about RDF triples). When querying CM-Well, you can specify the format in which you want to receive query results (the output format). When updating CM-Well, you can specify the encoding type in which you are providing the new information (the input format).

[The format Parameter](#hdr1)

[Formats by API Call](#hdr2)

[Format Descriptions](#hdr3)

[Format Examples](#hdr4)

<a name="hdr1"></a>
## The format Parameter ##

Depending on the specific API call, the **format** parameter indicates either the input or the output format.

For an API call directed to the `_in` endpoint, the **format** parameter indicates the format of the *input* you provide. If the **format** parameter is not provided, CM-Well tries to infer the input format from the HTTP Content-Type header.

For API calls directed to any other endpoint (`_out`, `_sp`, `_consume`, or an infoton URI), the **format** parameter indicates the format of the *output* that CM-Well sends in the response. If the **format** parameter is not provided, CM-Well tries to infer the output format from the HTTP Accept header. 

In some cases, an API call to the `_out` endpoint may have both formatted input and formatted output. For instance, you may provide a list of CM-Well paths as input to a query, and receive a list of infotons as output. In this case, the output format is indicated by the **format** parameter value, and the input format is indicated by the Content-Type header. The input format (the Content-Type header value) can be one of two values: plain text (a list of paths separated by new-line characters) or JSON.

<a name="hdr2"></a>
## Formats by API Call ##

The following table summarizes the format values supported by each type of CM-Well API call.
See [Format Descriptions](#hdr3) to learn more about each format type.

Category |	API calls |	Format refers to: |	Supported formats
:-------|:---------------|:----------|:-----
Get single infoton | Get single infoton | Output | csv, html, json, jsonl, jsonld, jsonldq, n3, ntriples, nquads, rdfxml, text/path, trig, trix, tsv, turtle/ttl,   yaml
Get collections | Search, get multiple infotons, get infoton/s with-history, iterate, consume, subscribe (pull/push) | Output | atom, csv, json, jsonl, jsonld, jsonldq, n3, ntriples, nquads, rdfxml, text/path, trig, trix, tsv, turtle/ttl,  yaml
SPARQL | SPARQL queries | Output | ascii, json, rdf, tsv, xml
Streaming | Stream | Output | json, jsonl, jsonld, jsonldq, tsv/tab, text/path
Updates | Add, delete and replace infotons and fields | Input | json, n3, ntriples, nquads, trig, trix, turtle/ttl, rdfxml


<a name="hdr3"></a>
## Format Descriptions ##

The following table describes the input and output formats that CM-Well supports:

Format | CM-Well Value | Description | Notes
:-------|:---------------|:----------|:-----
Atom | atom | HTML-formatted atom. | If a call includes `format=atom&with-data`, its output is an xslt with embedded data, so a browser can render it. You can also specify a format value for the atom data in the **with-data** parameter (e.g. `format=atom&with-data=n3`). Atom is the default format for search, iterate, and get-with-history operations.
CSV | csv | Comma-Separated Values, one header line, then one line per infoton. | Depending on whether the infotons have the same fields, the table may be sparsely populated with values (it's recommended to use the **fields** parameter to retrieve fields common to all infotons). For fields with more than one value, the field header is repeated with an index (e.g. Name1, Name2,...). The field order is arbitrary and may not be the same for every call.
HTML | html | HTML format, browser-renderable. | Only supported when retrieving a single infoton. For this case, HTML is the default format.
JSON | json | JSON (JavaScript Object Notation) format. Default for **consume** and **push/pull** operations, and for all operations directed to the `_out` endpoint. | Field names should be formatted as <fieldName>.<prefix> (e.g. "CommonName.mdaas"). Add the **pretty** flag for a more readable, tabulated format. When using one of the JSON formats, if you pass a function in the **callback** parameter, the callback function is called with the response, which is JSONP-formatted.
JSONL | jsonl | An expansion of JSON with quad and complex type information. | See JSON.
JSONLD | jsonld | JSON for Linked Data. Allows data to be serialized in a way that is similar to traditional JSON. | See JSON.
JSONLDQ | jsonldq | The same as JSONLD, but includes quad values. | See JSON.
RDF N3 | n3 | Short for "Notation3". Similar format to Turtle, but provides additional support for defining inference rules. | 
RDF NQuads | nquads | Similar to NTriples, but with a 4th value that is the sub-graph the triple belongs to. |
RDF Ntriples | ntriples| A simple, one-statement-per-line format, easy for a computer to consumer, and relatively readable by humans. |
RDF TriG | trig | Human-readable, with a compact syntax for expressing named graphs. |
RDF TriX | trix | Similar to TriG but XML-based. |
RDF Turtle | turtle or ttl | A refinement of NTriples, more compact and readable. 
RDF XML | rdfxml | The original format recommended by the RDF specification. Not very human-readable. |
Text | text or path | A simple format that only includes infoton paths, each as a single line of text (used mainly for streaming APIs). |
TSV | tsv or tab | Tab-Separated Values that include the infoton's path, lastModified, uuid and indexTime values in one line per infoton (used mainly for streaming APIs). |
YAML | yaml | A human-readable format that uses indentation to express data hierarchy. |


<a name="hdr4"></a>
## Format Examples ##

Here are some examples of RDF in different encoding formats: 

## N3 ##

    @prefix : <http://www.w3.org/2000/10/swap/test/demo1/about-pat#> .
    @prefix bio: <http://www.w3.org/2000/10/swap/test/demo1/biology#> .
    @prefix per: <http://www.w3.org/2000/10/swap/test/demo1/friends-vocab#> .
    
    :pat a bio:Human;
     	per:name "Pat Smith",
      		  	 "Patrick Smith";
     	per:pet  [
     		a bio:Dog;
     		per:name "Rover" ] .

## N-Triples   ##

    @prefix : <http://www.w3.org/2000/10/swap/test/demo1/about-pat#> .
    @prefix bio: <http://www.w3.org/2000/10/swap/test/demo1/biology#> .
    @prefix per: <http://www.w3.org/2000/10/swap/test/demo1/friends-vocab#> .
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
    
    :pat rdf:type bio:Human.
    :pat per:name "Pat Smith".
    :pat per:name "Patrick Smith".
    :pat per:pat _:genid1.
    _:genid1 rdf:type bio:Dog.
    _:genid1 per:name "Rover".
      
## Turtle ##

    @prefix ns0: <http://www.w3.org/2000/10/swap/test/demo1/friends-vocab#> .
    
    <http://www.w3.org/2000/10/swap/test/demo1/about-pat#pat>
      a <http://www.w3.org/2000/10/swap/test/demo1/biology#Human> ;
      ns0:name "Pat Smith", "Patrick Smith" ;
      ns0:pat [
    		a <http://www.w3.org/2000/10/swap/test/demo1/biology#Dog> ;
    		ns0:name "Rover"
      ] .
     
## RDF-XML  ##

    <rdf:RDF xmlns="http://www.w3.org/2000/10/swap/test/demo1/about-pat#"
    	xmlns:bio="http://www.w3.org/2000/10/swap/test/demo1/biology#"
    	xmlns:per="http://www.w3.org/2000/10/swap/test/demo1/friends-vocab#"
    	xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
    
    	<bio:Human rdf:about="#pat">
    		<per:name>Pat Smith</per:name>
    		<per:name>Patrick Smith</per:name>
    		<per:pet>
    			<bio:Dog>
    				<per:name>Rover</per:name>
    			</bio:Dog>
    		</per:pet>
    	</bio:Human>
    </rdf:RDF>

## JSON-LD ##

    [
      {
    	"@id": "_:b0",
    	"@type": [
      		"http://www.w3.org/2000/10/swap/test/demo1/biology#Dog"
    	],
    	"http://www.w3.org/2000/10/swap/test/demo1/friends-vocab#name": [
      		{
    			"@value": "Rover"
      		}	
    	]
      },
      {
    	"@id": "http://www.w3.org/2000/10/swap/test/demo1/about-pat#pat",
    	"@type": [
      		"http://www.w3.org/2000/10/swap/test/demo1/biology#Human"
    	],
    	"http://www.w3.org/2000/10/swap/test/demo1/friends-vocab#name": [
      		{
    			"@value": "Pat Smith"
      		},
      		{
    			"@value": "Patrick Smith"
      		}
    	],
    	"http://www.w3.org/2000/10/swap/test/demo1/friends-vocab#pat": [
      		{
    			"@id": "_:b0"
      		}
    	]
      },
      {
    	"@id": "http://www.w3.org/2000/10/swap/test/demo1/biology#Dog"
      },
      {
    	"@id": "http://www.w3.org/2000/10/swap/test/demo1/biology#Human"
      }
    ]


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.QueryParameters.md)  

----
