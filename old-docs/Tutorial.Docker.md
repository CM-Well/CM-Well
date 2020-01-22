# A Brief CM-Well Docker Tutorial #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercisesTOC.md)  

----

This page describes how to get started with CM-Well, by running some basic workflows over a private CM-Well Docker instance.

First, learn about [CM-Well Docker and how to install and run it](Tools.UsingCM-WellDocker.md). 

After installing and running CM-Well Docker, run the following workflows:

1. [Add some infotons and their fields](#hdr1).
1. [Read the infotons you wrote](#hdr2).
1. [Query for infotons by field values](#hdr3).
1. [Update field values](#hdr4).
1. [Read the changed infotons and verify that their fields were updated](#hdr5).
1. [Delete some infotons and fields](#hdr6).
1. [Read the infotons again and verify the deletions](#hdr7).

>**Note:** If you're planning to use the Curl command, read [Using the Curl Utility to Call CM-Well](DevGuide.CurlUtility.md).

<a name="hdr1"></a>
## 1. Add some infotons and their fields ##

**Action:** Create 5 new infotons under the path example/Individuals: MamaBear, PapaBear, BabyBear1, BabyBear2 and BabyBear3. Each bear has a **hasName** field with a name value.

**Curl command:**

    curl -X POST "http://localhost:8080/_in?format=ntriples" --data-binary @input.txt

**File contents:**

    <http://example/Individuals/MamaBear> <http://purl.org/vocab/relationship/spouseOf> <http://example/Individuals/PapaBear> .
    <http://example/Individuals/MamaBear> <http://ont.thomsonreuters.com/bermuda/hasName> "Betty".
    <http://example/Individuals/PapaBear> <http://ont.thomsonreuters.com/bermuda/hasName> "Barney".
    <http://example/Individuals/BabyBear1> <http://purl.org/vocab/relationship/childOf> <http://example/Individuals/MamaBear>.
    <http://example/Individuals/BabyBear1> <http://purl.org/vocab/relationship/childOf> <http://example/Individuals/PapaBear>.
    <http://example/Individuals/BabyBear1> <http://ont.thomsonreuters.com/bermuda/hasName> "Barbara".
    <http://example/Individuals/BabyBear1> <http://purl.org/vocab/relationship/siblingOf> <http://example/Individuals/BabyBear2>.
    <http://example/Individuals/BabyBear1> <http://purl.org/vocab/relationship/siblingOf> <http://example/Individuals/BabyBear3>.
    <http://example/Individuals/BabyBear2> <http://purl.org/vocab/relationship/childOf> <http://example/Individuals/MamaBear>.
    <http://example/Individuals/BabyBear2> <http://purl.org/vocab/relationship/childOf> <http://example/Individuals/PapaBear>.
    <http://example/Individuals/BabyBear2> <http://ont.thomsonreuters.com/bermuda/hasName> "Bobby".
    <http://example/Individuals/BabyBear3> <http://purl.org/vocab/relationship/childOf> <http://example/Individuals/MamaBear>.
    <http://example/Individuals/BabyBear3> <http://purl.org/vocab/relationship/childOf> <http://example/Individuals/PapaBear>.
    <http://example/Individuals/BabyBear3> <http://ont.thomsonreuters.com/bermuda/hasName> "Bert".

**Response:**

    {"success":true}

<a name="hdr2"></a>
## 2. Read the infotons you wrote ##

**Action:** Read all infotons under example/Individuals, with their fields.

**Curl command:**

    curl "http://localhost:8080/example/Individuals?op=search&format=ttl&recursive&with-data" 

**Response:**

    @prefix nn:<http://localhost:8080/meta/nn#> .
    @prefix bermuda: <http://ont.thomsonreuters.com/bermuda/> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix rel:   <http://purl.org/vocab/relationship/> .
    @prefix sys:   <http://localhost:8080/meta/sys#> .
    
    <http://localhost:8080/example/Individuals/MamaBear>
        sys:dataCenter    "lh" ;
        sys:indexTime     "1470058071114"^^xsd:long ;
        sys:lastModified  "2016-08-01T13:27:49.241Z"^^xsd:dateTime ;
        sys:parent        "/example/Individuals" ;
        sys:path          "/example/Individuals/MamaBear" ;
        sys:type          "ObjectInfoton" ;
        sys:uuid          "e6f36b01bec464f9e2a8d8b690590e31" ;
        bermuda:hasName   "Betty" ;
        rel:spouseOf      <http://example/Individuals/PapaBear> .

    <http://localhost:8080/example/Individuals/BabyBear2>
        sys:dataCenter    "lh" ;
        sys:indexTime     "1470058071125"^^xsd:long ;
        sys:lastModified  "2016-08-01T13:27:49.241Z"^^xsd:dateTime ;
        sys:parent        "/example/Individuals" ;
        sys:path          "/example/Individuals/BabyBear2" ;
        sys:type          "ObjectInfoton" ;
        sys:uuid          "284a5a2438db15b5f8d9ef87795c0945" ;
        bermuda:hasName   "Bobby" ;
        rel:childOf       <http://example/Individuals/PapaBear> , <http://example/Individuals/MamaBear> .

    <http://localhost:8080/example/Individuals/BabyBear3>
        sys:dataCenter    "lh" ;
        sys:indexTime     "1470058071112"^^xsd:long ;
        sys:lastModified  "2016-08-01T13:27:49.239Z"^^xsd:dateTime ;
        sys:parent        "/example/Individuals" ;
        sys:path          "/example/Individuals/BabyBear3" ;
        sys:type          "ObjectInfoton" ;
        sys:uuid          "671d93482c72b51cef5afa18c71692a5" ;
        bermuda:hasName   "Bert" ;
        rel:childOf       <http://example/Individuals/PapaBear> , <http://example/Individuals/MamaBear> .

    [ sys:pagination  [ sys:first  <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-01T13%3A27%3A49.239Z&to=2016-08-01T13%3A27%3A49.242Z&length=5&offset=0> ;
                    sys:last   <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-01T13%3A27%3A49.239Z&to=2016-08-01T13%3A27%3A49.242Z&length=5&offset=5> ;
                    sys:self   <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-01T13%3A27%3A49.239Z&to=2016-08-01T13%3A27%3A49.242Z&length=5&offset=0> ;
                    sys:type   "PaginationInfo"
                  ] ;

    sys:results [ sys:fromDate  "2016-08-01T13:27:49.239Z"^^xsd:dateTime ;
                    sys:infotons  <http://localhost:8080/example/Individuals/MamaBear> , <http://localhost:8080/example/Individuals/BabyBear1> , <http://localhost:8080/example/Individuals/PapaBear> , <http://localhost:8080/example/Individuals/BabyBear3> , <http://localhost:8080/example/Individuals/BabyBear2> ;
                    sys:length    "5"^^xsd:long ;
                    sys:offset    "0"^^xsd:long ;
                    sys:toDate    "2016-08-01T13:27:49.242Z"^^xsd:dateTime ;
                    sys:total     "5"^^xsd:long ;
                    sys:type      "SearchResults"
                  ] ;
      sys:type"SearchResponse"
    ] .
    
    <http://localhost:8080/example/Individuals/BabyBear1>
        sys:dataCenter    "lh" ;
        sys:indexTime     "1470058071125"^^xsd:long ;
        sys:lastModified  "2016-08-01T13:27:49.242Z"^^xsd:dateTime ;
        sys:parent        "/example/Individuals" ;
        sys:path          "/example/Individuals/BabyBear1" ;
        sys:type          "ObjectInfoton" ;
        sys:uuid          "1627fe787d44b5a4fff19f50181b585b" ;
        bermuda:hasName   "Barbara" ;
        rel:childOf       <http://example/Individuals/MamaBear> , <http://example/Individuals/PapaBear> ;
        rel:siblingOf     <http://example/Individuals/BabyBear2> , <http://example/Individuals/BabyBear3> .

    <http://localhost:8080/example/Individuals/PapaBear>
        sys:dataCenter    "lh" ;
        sys:indexTime     "1470058071120"^^xsd:long ;
        sys:lastModified  "2016-08-01T13:27:49.241Z"^^xsd:dateTime ;
        sys:parent        "/example/Individuals" ;
        sys:path          "/example/Individuals/PapaBear" ;
        sys:type          "ObjectInfoton" ;
        sys:uuid          "6513a8d6395af8db932f49afb97cbfd1" ;
        bermuda:hasName   "Barney" .

<a name="hdr3"></a>
## 3. Query for infotons by field values ##

**Action:** Read all infotons under example/Individuals that have a childOf relationship to PapaBear.

**Curl command:**

    curl "http://localhost:8080/example/Individuals?op=search&qp=childOf.rel:PapaBear&format=ttl&recursive"

**Response:**

    @prefix nn:<http://localhost:8080/meta/nn#> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix sys:   <http://localhost:8080/meta/sys#> .
    
    <http://localhost:8080/example/Individuals/BabyBear2>
        sys:dataCenter    "lh" ;
        sys:indexTime     "1470058071125"^^xsd:long ;
        sys:lastModified  "2016-08-01T13:27:49.241Z"^^xsd:dateTime ;
        sys:parent        "/example/Individuals" ;
        sys:path          "/example/Individuals/BabyBear2" ;
        sys:type          "ObjectInfoton" ;
        sys:uuid          "284a5a2438db15b5f8d9ef87795c0945" .

    <http://localhost:8080/example/Individuals/BabyBear3>
        sys:dataCenter    "lh" ;
        sys:indexTime     "1470058071112"^^xsd:long ;
        sys:lastModified  "2016-08-01T13:27:49.239Z"^^xsd:dateTime ;
        sys:parent        "/example/Individuals" ;
        sys:path          "/example/Individuals/BabyBear3" ;
        sys:type          "ObjectInfoton" ;
        sys:uuid          "671d93482c72b51cef5afa18c71692a5" .

    [ sys:pagination  [ sys:first  <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-01T13%3A27%3A49.239Z&to=2016-08-01T13%3A27%3A49.242Z&qp=childOf.rel%3APapaBear&length=3&offset=0> ;
                    sys:last   <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-01T13%3A27%3A49.239Z&to=2016-08-01T13%3A27%3A49.242Z&qp=childOf.rel%3APapaBear&length=3&offset=3> ;
                    sys:self   <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-01T13%3A27%3A49.239Z&to=2016-08-01T13%3A27%3A49.242Z&qp=childOf.rel%3APapaBear&length=3&offset=0> ;
                    sys:type   "PaginationInfo"
                  ] ;

    sys:results [ sys:fromDate  "2016-08-01T13:27:49.239Z"^^xsd:dateTime ;
                    sys:infotons  <http://localhost:8080/example/Individuals/BabyBear3> , <http://localhost:8080/example/Individuals/BabyBear2> , <http://localhost:8080/example/Individuals/BabyBear1> ;
                    sys:length    "3"^^xsd:long ;
                    sys:offset    "0"^^xsd:long ;
                    sys:toDate    "2016-08-01T13:27:49.242Z"^^xsd:dateTime ;
                    sys:total     "3"^^xsd:long ;
                    sys:type      "SearchResults"
                  ] ;
      sys:type"SearchResponse"
    ] .
    
    <http://localhost:8080/example/Individuals/BabyBear1>
        sys:dataCenter    "lh" ;
        sys:indexTime     "1470058071125"^^xsd:long ;
        sys:lastModified  "2016-08-01T13:27:49.242Z"^^xsd:dateTime ;
        sys:parent        "/example/Individuals" ;
        sys:path          "/example/Individuals/BabyBear1" ;
        sys:type          "ObjectInfoton" ;
        sys:uuid          "1627fe787d44b5a4fff19f50181b585b" .

<a name="hdr4"></a>
## 4. Update field values ##

**Action:** Change all Baby Bear names.

**Curl command:**

    curl -X POST "http://localhost:8080/_in?format=ntriples" --data-binary @input.txt

**File contents:**

    <http://example/Individuals/BabyBear1> <cmwell://meta/sys#markReplace> <http://ont.thomsonreuters.com/bermuda/hasName> . 
    <http://example/Individuals/BabyBear1> <http://ont.thomsonreuters.com/bermuda/hasName> "Cathy" .
    <http://example/Individuals/BabyBear2> <cmwell://meta/sys#markReplace> <http://ont.thomsonreuters.com/bermuda/hasName> . 
    <http://example/Individuals/BabyBear2> <http://ont.thomsonreuters.com/bermuda/hasName> "Craig" .
    <http://example/Individuals/BabyBear3> <cmwell://meta/sys#markReplace> <http://ont.thomsonreuters.com/bermuda/hasName> . 
    <http://example/Individuals/BabyBear3> <http://ont.thomsonreuters.com/bermuda/hasName> "Curt" .

**Response:**

    {"success":true}

<a name="hdr5"></a>
## 5. Read the changed infotons and verify that their fields were updated ##

**Action:** Read the Baby Bear infotons and verify that their fields were updated.

**Curl command:**

    curl "http://localhost:8080/example/Individuals?op=search&qp=childOf.rel:PapaBear&format=ttl&recursive&with-data"

**Response:**

    @prefix nn:<http://localhost:8080/meta/nn#> .
    @prefix bermuda: <http://ont.thomsonreuters.com/bermuda/> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix rel:   <http://purl.org/vocab/relationship/> .
    @prefix sys:   <http://localhost:8080/meta/sys#> .
    
    <http://localhost:8080/example/Individuals/BabyBear2>
        sys:dataCenter    "lh" ;
        sys:indexTime     "1470063470479"^^xsd:long ;
        sys:lastModified  "2016-08-01T14:57:49.993Z"^^xsd:dateTime ;
        sys:parent        "/example/Individuals" ;
        sys:path          "/example/Individuals/BabyBear2" ;
        sys:type          "ObjectInfoton" ;
        sys:uuid          "ad07b30c8ba4ea88d5702872c4146fab" ;
        bermuda:hasName   "Craig" ;
        rel:childOf       <http://example/Individuals/PapaBear> , <http://example/Individuals/MamaBear> .

    <http://localhost:8080/example/Individuals/BabyBear3>
        sys:dataCenter    "lh" ;
        sys:indexTime     "1470063470478"^^xsd:long ;
        sys:lastModified  "2016-08-01T14:57:49.992Z"^^xsd:dateTime ;
        sys:parent        "/example/Individuals" ;
        sys:path          "/example/Individuals/BabyBear3" ;
        sys:type          "ObjectInfoton" ;
        sys:uuid          "54ec65cd8e06c19b9007b8b5fb60ba71" ;
        bermuda:hasName   "Curt" ;
        rel:childOf       <http://example/Individuals/PapaBear> , <http://example/Individuals/MamaBear> .

    [ sys:pagination  [ sys:first  <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-01T14%3A57%3A49.992Z&to=2016-08-01T14%3A57%3A49.993Z&qp=childOf.rel%3APapaBear&length=3&offset=0> ;
                    sys:last   <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-01T14%3A57%3A49.992Z&to=2016-08-01T14%3A57%3A49.993Z&qp=childOf.rel%3APapaBear&length=3&offset=3> ;
                    sys:self   <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-01T14%3A57%3A49.992Z&to=2016-08-01T14%3A57%3A49.993Z&qp=childOf.rel%3APapaBear&length=3&offset=0> ;
                    sys:type   "PaginationInfo"
      ] ;
     sys:results [ sys:fromDate  "2016-08-01T14:57:49.992Z"^^xsd:dateTime ;
                    sys:infotons  <http://localhost:8080/example/Individuals/BabyBear3> , <http://localhost:8080/example/Individuals/BabyBear1> , <http://localhost:8080/example/Individuals/BabyBear2> ;
                    sys:length    "3"^^xsd:long ;
                    sys:offset    "0"^^xsd:long ;
                    sys:toDate    "2016-08-01T14:57:49.993Z"^^xsd:dateTime ;
                    sys:total     "3"^^xsd:long ;
                    sys:type      "SearchResults"
                  ] ;
      sys:type"SearchResponse"
    ] .
    
    <http://localhost:8080/example/Individuals/BabyBear1>
        sys:dataCenter    "lh" ;
        sys:indexTime     "1470063470479"^^xsd:long ;
        sys:lastModified  "2016-08-01T14:57:49.993Z"^^xsd:dateTime ;
        sys:parent        "/example/Individuals" ;
        sys:path          "/example/Individuals/BabyBear1" ;
        sys:type          "ObjectInfoton" ;
        sys:uuid          "70e303b295d4abdb68f9197ca76531cc" ;
        bermuda:hasName   "Cathy" ;
        rel:childOf       <http://example/Individuals/MamaBear> , <http://example/Individuals/PapaBear> ;
        rel:siblingOf     <http://example/Individuals/BabyBear2> , <http://example/Individuals/BabyBear3> .

<a name="hdr6"></a>
## 6. Delete some infotons and fields ##

**Action:** Delete the BabyBear3 infoton, and delete BabyBear1's siblingOf field.

**Curl command:**

    curl -X POST "http://localhost:8080/_in?format=ntriples" --data-binary @input.txt

**File contents:**

    <http://example/Individuals/BabyBear3> <cmwell://meta/sys#fullDelete> "false" .
    <http://example/Individuals/BabyBear1> <cmwell://meta/sys#markReplace> <http://purl.org/vocab/relationship/siblingOf>.

**Response:**

    {"success":true}

<a name="hdr7"></a>
## 7. Read the infotons again and verify the deletions ##

**Action:** Read the Baby Bear infotons again and verify the deletions.

**Curl command:**

    curl "http://localhost:8080/example/Individuals?op=search&qp=$http://purl.org/vocab/relationship/childOf$:PapaBear&format=ttl&recursive&with-data"

**Response:**

    @prefix nn:<http://localhost:8080/meta/nn#> .
    @prefix bermuda: <http://ont.thomsonreuters.com/bermuda/> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix sys:   <http://localhost:8080/meta/sys#> .
    @prefix relationship: <http://purl.org/vocab/relationship/> .
    
    <http://localhost:8080/example/Individuals/BabyBear2>
        sys:dataCenter        "dc1" ;
        sys:indexTime         "1470126116579"^^xsd:long ;
        sys:lastModified      "2016-08-02T08:21:54.723Z"^^xsd:dateTime ;
        sys:parent            "/example/Individuals" ;
        sys:path              "/example/Individuals/BabyBear2" ;
        sys:type              "ObjectInfoton" ;
        sys:uuid              "e53aca37ccc41b075eb74902e3f9c1ec" ;
        bermuda:hasName       "Craig" ;
        relationship:childOf  <http://example/Individuals/PapaBear> , <http://example/Individuals/MamaBear> .

    [ sys:pagination  [ sys:first  <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-02T08%3A21%3A54.723Z&to=2016-08-02T08%3A25%3A47.780Z&qp=%24http%3A%2F%2Fpurl.org%2Fvocab%2Frelationship%2FchildOf%24%3APapaBear&length=2&offset=0> ;
                    sys:last   <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-02T08%3A21%3A54.723Z&to=2016-08-02T08%3A25%3A47.780Z&qp=%24http%3A%2F%2Fpurl.org%2Fvocab%2Frelationship%2FchildOf%24%3APapaBear&length=2&offset=2> ;
                    sys:self   <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-02T08%3A21%3A54.723Z&to=2016-08-02T08%3A25%3A47.780Z&qp=%24http%3A%2F%2Fpurl.org%2Fvocab%2Frelationship%2FchildOf%24%3APapaBear&length=2&offset=0> ;
                    sys:type   "PaginationInfo"
                  ] ;

    sys:results [ sys:fromDate  "2016-08-02T08:21:54.723Z"^^xsd:dateTime ;
                    sys:infotons  <http://localhost:8080/example/Individuals/BabyBear2> , <http://localhost:8080/example/Individuals/BabyBear1> ;
                    sys:length    "2"^^xsd:long ;
                    sys:offset    "0"^^xsd:long ;
                    sys:toDate    "2016-08-02T08:25:47.780Z"^^xsd:dateTime ;
                    sys:total     "2"^^xsd:long ;
                    sys:type      "SearchResults"
                  ] ;
      sys:type"SearchResponse"
    ] .
    
    <http://localhost:8080/example/Individuals/BabyBear1>
        sys:dataCenter        "dc1" ;
        sys:indexTime         "1470126348410"^^xsd:long ;
        sys:lastModified      "2016-08-02T08:25:47.780Z"^^xsd:dateTime ;
        sys:parent            "/example/Individuals" ;
        sys:path              "/example/Individuals/BabyBear1" ;
        sys:type              "ObjectInfoton" ;
        sys:uuid              "90e936f386a580c559a4919647edcae6" ;
        bermuda:hasName       "Cathy" ;
        relationship:childOf  <http://example/Individuals/MamaBear> , <http://example/Individuals/PapaBear> .

## API Reference ##
* [Add Infotons and Fields](API.Update.AddInfotonsAndFields.md)
* [Delete Multiple Infotons](API.Update.DeleteMultipleInfotons.md)
* [Delete Fields](API.Update.DeleteFields.md)
* [Get Multiple Infotons by URI](API.Get.GetMultipleInfotonsByURI.md)
* [Query for Infotons Using Field Conditions](API.Query.QueryForInfotonsUsingFieldConditions.md)
* [Replace Field Values](API.Update.ReplaceFieldValues.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercisesTOC.md)  

----