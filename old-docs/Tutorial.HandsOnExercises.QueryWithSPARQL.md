# Hands-On Exercise: Query with SPARQL #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.QueryForInfotons.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.QueryWithGremlin.md)  

----

**Action:** Search for organizations whose name contains the string "Systems", and apply SPARQL to the results to display the infotons' URIs, and the organization names and addresses.

**Curl command:**

    curl -X POST "<cm-well-host>/_sp?format=ascii" -H "Content-Type:text/plain" --data-binary @inputfile.txt

**Input file contents:**

    PATHS
    /permid.org?op=search&qp=CommonName.mdaas:Systems&with-data
    
    SPARQL
    SELECT * WHERE {
    ?Infoton <http://ont.thomsonreuters.com/mdaas/CommonName> ?Name.
    ?Infoton <http://ont.thomsonreuters.com/mdaas/headquartersCommonAddress> ?Address.
    }

**Response:**

	--------------------------------------------------------------------------------------------------------------------------------------------------------------
    | Infoton  | Name  | Address|
    ==============================================================================================================================================================
    | <http://permid.org/1-5044348019> | "Tiller Systems SAS"  | "France\n"  |
    | <http://permid.org/1-5048323276> | "Expert Systems Holdings Ltd" | "17/F., AXA Tower, Landmark East, 100 How Ming Street, Kwun Tong, Kowloon\nHong Kong\n" |
    | <http://permid.org/1-5050714197> | "Keyless Systems Ltd" | "Israel\n"  | 
    --------------------------------------------------------------------------------------------------------------------------------------------------------------
        
## API Reference ##
[Using SPARQL on Infotons](DevGuide.UsingSPARQLOnCM-WellInfotons.md)
[Apply SPARQL to Query Results](API.Query.ApplySPARQLToQueryResults.md)
[Field Condition Syntax](API.FieldConditionSyntax.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.QueryForInfotons.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.QueryWithGremlin.md)  

----