# Function: *Replace Field Values* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.AddInfotonsAndFields.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.DeleteASingleInfoton.md)  

----

## Description ##
You can replace existing field values with new values, using a POST command to the _in endpoint.

There are two ways to do this: 

1. Use the **replace-mode** flag.
2. Use the special **#markReplace** indicator.

>**Note:** The **replace-mode** flag and the **#markReplace** indicator should not be used in the same command. If they are, **replace-mode** overrides **#markReplace**.

Both methods are described in the following sections.

## Syntax 1 ##

**URL:** <cm-well-host>/_in
**REST verb:** POST
**Mandatory parameters:** replace-mode

----------

**Template:**

    curl -X POST "<cm-well-host>/_in?format=<format>&replace-mode" <triples with new field values>

**URL example:** N/A

**Curl example (REST API):**

    curl -X POST "<cm-well-host>/_in?format=ttl&replace-mode" -H "Content-Type: text/plain" --data-binary @curlInput.txt

### File Contents ###

    <http://data.com/1-12345678> 
    <http://ont.com/bermuda/hasName> "John Smith" .

## Parameters 1 ##

<table>
  <tr>
    <th align=left><h3><i>Parameter</i></h3></th>
    <th align=left><h3><i>Description</i></h3></th>
	<th align=left><h3><i>Values</i></h3></th>
	<th align=left><h3><i>Example</i></h3></th>
	<th align=left><h3><i>Reference</i></h3></th>
  </tr>
	<tr>
		<td>format</td>
		<td>The format in which the triples are provided</td>
		<td>n3, rdfxml, ntriples, nquads, turtle/ttl, trig, trix</td>
		<td>format=n3/td>
		<td><a href="API.InputAndOutputFormats.md">CM-Well Input and Output Formats</a></td>
	</tr>
	<tr>
	    <td rowspan="5" align=left>replace-mode</td>
		<td colspan="4">Indicates that the new field value should replace the existing value, rather than be written as an additional value. See parameter value options below.</td>
	</tr>
		<td>No value.</td>
		<td>replace-mode</td>
		<td>Replaces the field values that have the same quad value as the fields you're uploading.</td>
		<td>N/A</td>
	<tr>
		<td>Default. </td>
		<td>replace-mode=default</td>
		<td>Replaces field values in the default graph (fields that have no quad value).</td>
		<td>N/A</td>
	</tr>
		<td>Specific graph label.</td>
		<td>replace-mode=http://some-graph/uri</td>
		<td>Replaces field values that have the specified quad (graph label) value.</td>
		<td>N/A</td>
	<tr>
		<td>All</td>
		<td>replace-mode=*</td>
		<td>Replaces all field values regardless of their quad value.</td>
		<td>N/A</td>
	</tr>
</table>

## Code Example 1 ##

### Call ###

    curl -X POST "<cm-well-host>/_in?format=ttl&replace-mode" -H "Content-Type: text/plain" --data-binary @curlInput.txt

### File Contents ###
    <http://data.com/1-12345678> 
    <http://ont.com/bermuda/hasName> "John Smith" .

### Results ###

    {"success":true}

## Syntax 2 ##

**URL:** <CMWellHost>/_in
**REST verb:** POST
**Mandatory parameters:** None

----------

**Template:**

    curl -X POST <cm-well-host>/_in?format=<format> <triples with new field values>

**URL example:** N/A

**Curl example (REST API):**

    curl -X POST "<cm-well-host>/_in?format=ttl " -H "Content-Type: text/plain" --data-binary @curlInput.txt

### File Contents ###

    <http://data.com/1-12345678> 
    <cmwell://meta/sys#markReplace> <http://ont.com/bermuda/hasName> . 
    <http://data.com/1-12345678> <http://ont.com/bermuda/hasName> "John Doe" . 

>**Note:** For each field we want to replace, we supply two triples: one that indicates that the previous value should be deleted (using the #markReplace indicator), and one that provides the new value. You can supply several such pairs of triples, to replace several fields in the same call. You can also mix these pairs with other triples that just add values without deleting.

## Parameters 2 ##

Parameter | Description | Values | Example | Reference
:----------|:-------------|:--------|:---------|:----------
format | The format in which the triples are provided | n3, rdfxml, ntriples, nquads, turtle/ttl, trig, trix | format=n3 | [CM-Well Input and Output Formats](API.InputAndOutputFormats.md)

## Code Example 2 ##

### Call ###

    curl -X POST "<cm-well-host>/_in?format=ttl&replace-mode" -H "Content-Type: text/plain" --data-binary @curlInput.txt

### File Contents ###

    <http://data.com/1-12345678> 
    <http://ont.com/bermuda/hasName> "John Smith" .

### Results ###

    {"success":true}

## Notes ##

* For best performance, if you're performing several replacements in one call, group the updates by subject, meaning that several triples referring to the same RDF subject should appear in sequence in the request payload. 
* If you add a value for a field that already has a value, the old value is not overwritten. Instead, the infoton will have two triples with the same field name but different values. To overwrite a field value, you must use the **replace** API calls described above.
* If you want to replace field values, use the replace options rather than deleting values and adding new values in two separate calls. This is because each call that updates an infoton creates a new historical infoton version. The interim version with deleted fields just takes up storage needlessly.
* If you attempt to perform an update on an infoton, where all of the "new" infotons triples are identical to the existing infoton's triples, no update will occur. This is in order to prevent saving a new infoton version when no actual change was made.

## Related Topics ##
[Add Infotons and Fields](API.Update.AddInfotonsAndFields.md)
[Delete Fields](API.Update.DeleteFields.md)
[Replace a Named Sub-Graph](API.Update.DeleteOrReplaceValuesInNamedSubGraph.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.AddInfotonsAndFields.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.DeleteASingleInfoton.md)  

----