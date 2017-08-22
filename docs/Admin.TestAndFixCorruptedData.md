# Testing and Fixing Corrupted Data #

Occasionally we have found errors in how infotons are stored and indexed. These used to arise from race conditions that happened when several requests to update the same infoton were submitted nearly simultaneously. In these cases, there could be discrepancies between infotons stored in Cassandra (CM-Well's 3rd-party storage module) and how they're indexed in Elastic Search (CM-Well's 3rd-party indexing  and search module). For instance, the same infoton could have more than one index entry, or the current (latest) infoton could point to more than one UUID. These errors can cause different types of problems, such as duplicate, missing or inconsistent search results.

CM-Well supports a set of utility functions for testing and fixing such cases of index corruption on a single infoton. The functions are described in the following sections.

>**Notes:** 
>* CM-Well has since been improved to prevent these race conditions, and this should not happen going forward. However, in the older CM-Well data schema, you may still encounter infotons whose indexes have already been corrupted.
>* The data test and fix functions should be used with caution, and can be disruptive in terms of the system resources they use. Use with care! They are likely to be deprecated in the future, when they are no longer needed. 

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

    <cm-well-host>/11-123RT12?op=x-info

Response:

	es  445ec58994f5be1b61b0bfb65395d751 [cmwell_current_1(1)] {"type":"ObjectInfoton","system":{"path":"/data.thomsonreuters.com/11-123RT12","lastModified":"2015-08-13T14:31:43.411Z","uuid":"445ec58994f5be1b61b0bfb65395d751","parent":"/data.thomsonreuters.com","dc":"dc1","indexTime":1439476304965,"quad":[]},"fields":{"LEI.1ZNHcw":["549300BGA2FRV4OQQV27"],"DunsNumber.1ZNHcw":["888888"],"createdByFirstName.1ZNHcw":["Testy"],"organizationTier.1ZNHcw":["4"],"NDAID.1ZNHcw":["107970379"],"organizationSubtypeCode.1ZNHcw":["21"],"TmtCompanyId.1ZNHcw":["C902973043"],"createdByGroupCode.1ZNHcw":["Super Administrator"],"InvestextId.1ZNHcw":["YYASEN"],"isPublic.1ZNHcw":["0"],"organizationNameLocalLng.1ZNHcw":["xxx"],"organizationNameLocalDba.1ZNHcw":["abc company - RT"],"isIncorporatedIn.1ZNHcw":["US"],"modifiedByAnalystId.1ZNHcw":["2110"],"TaxID.1ZNHcw":["200656482"],"RegulatoryID.1ZNHcw":["NA01"],"PI.1ZNHcw":["17763959"],"modifiedByGroupCode.1ZNHcw":["Super Administrator"],"MXID.1ZNHcw":["108983157"],"organizationNameNormalizedLng.1ZNHcw":["xxx"],"CIDGEN.1ZNHcw":["749872001"],"RegistrationNumber.1ZNHcw":["20001079167"],"SDCCUSIP.1ZNHcw":["04576N"],"GemAlphaNumericId.1ZNHcw":["CT01Z1992H"],"providerId.1ZNHcw":["101"],"organizationTypeCode.1ZNHcw":["8"],"modifiedByLastName.1ZNHcw":["Zoomerson"],"organizationNameLocalShtEnglish.1ZNHcw":["xxx"],"type.lzN1FA":["http://ontology.thomsonreuters.com/fedapioa/Organization"],"createdByLastName.1ZNHcw":["Testerson"],"isParentOf.1ZNHcw":["2"],"isDomiciledIn.1ZNHcw":["US"],"adminStatus.1ZNHcw":["0"],"organizationStatusCode.1ZNHcw":["1"],"organizationNameTransliteratedLng.1ZNHcw":["xxx"],"modifiedByFirstName.1ZNHcw":["Zoomy"],"createdByAnalystId.1ZNHcw":["123"],"organizationId.1ZNHcw":[12300012],"isUltimateParentOf.1ZNHcw":["2"],"RCPIDx.1ZNHcw":["600147232"],"EFXId.1ZNHcw":["11613527"]}}
    cas 445ec58994f5be1b61b0bfb65395d751 {"type":"ObjectInfoton","system":{"uuid":"445ec58994f5be1b61b0bfb65395d751","lastModified":"2015-08-13T14:31:43.411Z","path":"/data.thomsonreuters.com/11-123RT12","dataCenter":"dc1","indexTime":1460409303057,"parent":"/data.thomsonreuters.com"},"fields":{"LEI.fedapioa":["549300BGA2FRV4OQQV27"],"DunsNumber.fedapioa":["888888"],"createdByFirstName.fedapioa":["Testy"],"organizationTier.fedapioa":["4"],"NDAID.fedapioa":["107970379"],"organizationSubtypeCode.fedapioa":["21"],"TmtCompanyId.fedapioa":["C902973043"],"createdByGroupCode.fedapioa":["Super Administrator"],"InvestextId.fedapioa":["YYASEN"],"isPublic.fedapioa":["0"],"organizationNameLocalLng.fedapioa":["xxx"],"entityCreatedDate.fedapioa":["2009-07-26 03:37:16"],"organizationNameLocalDba.fedapioa":["abc company - RT"],"isIncorporatedIn.fedapioa":["US"],"modifiedByAnalystId.fedapioa":["2110"],"TaxID.fedapioa":["200656482"],"RegulatoryID.fedapioa":["NA01"],"PI.fedapioa":["17763959"],"modifiedByGroupCode.fedapioa":["Super Administrator"],"MXID.fedapioa":["108983157"],"organizationNameNormalizedLng.fedapioa":["xxx"],"CIDGEN.fedapioa":["749872001"],"RegistrationNumber.fedapioa":["20001079167"],"SDCCUSIP.fedapioa":["04576N"],"GemAlphaNumericId.fedapioa":["CT01Z1992H"],"providerId.fedapioa":["101"],"organizationTypeCode.fedapioa":["8"],"modifiedByLastName.fedapioa":["Zoomerson"],"organizationNameLocalShtEnglish.fedapioa":["xxx"],"type.rdf":["http://ontology.thomsonreuters.com/fedapioa/Organization"],"createdByLastName.fedapioa":["Testerson"],"entityLastReviewedDate.fedapioa":["2013-06-21 00:00:00"],"isParentOf.fedapioa":["2"],"isDomiciledIn.fedapioa":["US"],"adminStatus.fedapioa":["0"],"organizationStatusCode.fedapioa":["1"],"organizationNameTransliteratedLng.fedapioa":["xxx"],"modifiedByFirstName.fedapioa":["Zoomy"],"createdByAnalystId.fedapioa":["123"],"organizationId.fedapioa":[12300012],"entityModifiedDate.fedapioa":["2014-09-15 02:24:42"],"isUltimateParentOf.fedapioa":["2"],"RCPIDx.fedapioa":["600147232"],"EFXId.fedapioa":["11613527"]}}
    
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

