# Testing and Fixing Corrupted Data #

Occasionally we have found errors in how infotons are stored and indexed. These used to arise from race conditions that happened when several requests to update the same infoton were submitted nearly simultaneously. In these cases, there could be discrepancies between infotons stored in Cassandra (CM-Well's 3rd-party storage module) and how they're indexed in Elastic Search (CM-Well's 3rd-party indexing  and search module). For instance, the same infoton could have more than one index entry, or the current (latest) infoton could point to more than one UUID. These errors can cause different types of problems, such as duplicate, missing or inconsistent search results.

CM-Well supports a set of utility functions for testing and fixing such cases of index corruption on a single infoton. The functions are described in the following sections.

>**Notes:** 
>* CM-Well has since been improved to prevent these race conditions, and this should not happen going forward. However, in the older CM-Well data schema, you may still encounter infotons whose indexes have already been corrupted.
>* The data test and fix functions should be used with caution, and can be disruptive in terms of the system resources they use. They are intended to be run by CM-Well support personnel, or advanced users *after consulting with the CM-Well support team*. They are likely to be deprecated in the future, when they are no longer needed. 

## The x-verify Function ##

You can run **x-verify** on a single infoton, to determine whether its indexing is valid or not. A return value of **true** means the index is valid, while **false** means it is not. If you receive a **false** result, you can fix the data corruption by running the **x-fix** function.

The **x-verify** function takes the following *optional* parameters:

Parameter | Description
:----|:-----------
versions-limit | You can provide a numeric value to this parameter, in order to limit the number of versions that are verified. For example, adding **versions-limit=200** to the **x-verify**  request causes only the first 200 versions to be verified. 


**Example:**

Request:

    <cm-well-host>/example.com/2-9B50FC1DEBB771CABB59ADEA33CBA034F82435BA3D1FD6A6CFA97F2273172E54?op=x-verify

Response:

    {"type":"SimpleResponse","success":true}

## The x-info Function ##

When you run the **x-info** function on a single infoton, the results contain all Elastic Search index entries and all Cassandra storage entries for all versions of the given infoton. One line is returned for each entry.

The Cassandra entries are preceded by the string **"cas"**, and contain the version's data fields and metadata (system) fields. The Elastic Search entries are preceded by the string **"es"**, and contain the version's UUID and the index (or indices, in case of error) that contain this UUID.

You can examine this information to detect indexing and storage problems. 

**Example:**

Request:

    <cm-well-host>/example.com/2-9B50FC1DEBB771CABB59ADEA33CBA034F82435BA3D1FD6A6CFA97F2273172E54?op=x-info

Response:

	cas c4f0ed81b2d414edbbb855127bc8aafc {"type":"ObjectInfoton","system":{"uuid":"c4f0ed81b2d414edbbb855127bc8aafc","lastModified":"2016-04-07T15:33:35.394Z","path":"/demo.elite.thomsonreuters.com/2-9B50FC1DEBB771CABB59ADEA33CBA034F82435BA3D1FD6A6CFA97F2273172E54","dataCenter":"dc1","indexTime":1460043219439,"parent":"/demo.elite.thomsonreuters.com"},"fields":{"oDOfficerIsDirector.OfficersDirectors":["1"],"oDOfficerIsOfficer.OfficersDirectors":["0"],"oDPersonId.OfficersDirectors":["http://demo.elite.thomsonreuters.com/1-34413157710"],"type.rdf":["http://OfficersDirectors.schemas.financial.thomsonreuters.com/OfficersDirectors/2014-10-10/OfficerInformation"],"oDOrganizationId.OfficersDirectors":["http://permid.org/1-4295989904"]}}
    es  c4f0ed81b2d414edbbb855127bc8aafc: [cmwell_history_8]
    cas cf0b7db11a067cda779d1341cc911638 {"type":"ObjectInfoton","system":{"uuid":"cf0b7db11a067cda779d1341cc911638","lastModified":"2017-01-17T07:03:33.505Z","path":"/demo.elite.thomsonreuters.com/2-9B50FC1DEBB771CABB59ADEA33CBA034F82435BA3D1FD6A6CFA97F2273172E54","dataCenter":"dc1","indexTime":1484636614911,"parent":"/demo.elite.thomsonreuters.com"},"fields":{"oDOfficerIsDirector.OfficersDirectors":["1"],"oDOfficerIsOfficer.OfficersDirectors":["0"],"oDPersonId.OfficersDirectors":["http://demo.elite.thomsonreuters.com/1-34413157710"],"randomTestField.OfficersDirectors":[3.1415927],"type.rdf":["http://OfficersDirectors.schemas.financial.thomsonreuters.com/OfficersDirectors/2014-10-10/OfficerInformation"],"oDOrganizationId.OfficersDirectors":["http://permid.org/1-4295989904"]}}
    es  cf0b7db11a067cda779d1341cc911638: [cmwell_current_9]
    
## The x-fix Function ##

If you have detected a corrupted index for a certain infoton by running **x-verify** and/or **x-info**, you can fix the corruption by running **x-fix**. **x-fix** fixes data and indexing problems, and deletes any invalid infotons or index data.

The **x-fix** function takes the following *optional* parameters:

Parameter | Description
:----|:-----------
reactive | In some cases where a single infoton has a very large number of historical versions (due to improper usage), the **x-fix** operation may time out. In this case, it is recommended to add the **reactive** flag to the request (see the example below). Using this Boolean flag causes CM-Well to process the infoton versions in small batches rather than all at once, preventing the memory problems.
parallelism | Setting this parameter's value to more than 1 causes infoton versions to be processed in parallel, making the fix operation faster. For example, adding **parallelism=4** to the **x-fix**  request causes the infoton versions to be divided into 4 "buckets", which are processed in parallel.
versions-limit | You can provide a numeric value to this parameter, in order to limit the number of versions that are processed during the fix operation. For example, adding **versions-limit=200** to the **x-fix**  request causes only the first 200 versions to be processed. **Note:** The **versions-limit** parameter is ignored if the **reactive** flag is used.

**Example:**

Request (using the **reactive** flag):

    <cm-well-host>/example.com/2-9B50FC1DEBB771CABB59ADEA33CBA034F82435BA3D1FD6A6CFA97F2273172E54?op=x-fix&reactive

Response:

	{"type":"SimpleResponse","success":true,"message":"2015-11-06T01:35:18.700Z [1446773718700]"}
    {"type":"SimpleResponse","success":true,"message":"2015-11-06T01:35:20.411Z [1446773720411]"}
    {"type":"SimpleResponse","success":true,"message":"2017-01-17T08:01:35.182Z [1484640095182]"}
    {"type":"SimpleResponse","success":true,"message":"2017-01-17T08:03:44.584Z [1484640224584]"}
    {"type":"SimpleResponse","success":true,"message":"2017-01-17T08:03:45.583Z [1484640225583]"}
    {"type":"SimpleResponse","success":true,"message":"2017-01-17T08:03:46.541Z [1484640226541]"}
    {"type":"SimpleResponse","success":true,"message":"2017-01-17T08:03:46.663Z [1484640226663]"}
    {"type":"SimpleResponse","success":true,"message":"2017-01-17T08:03:47.641Z [1484640227641]"}
    {"type":"SimpleResponse","success":true,"message":"2017-01-17T08:03:48.638Z [1484640228638]"}

>**Note:** Each line in the response represents a chunk of data (several infotons). The date/time is the last-modified time of the infotons in the relevant chunk. If there is an error in a chunk, its **success** value is **false**, and the **message** field contains an informative message describing the error.

