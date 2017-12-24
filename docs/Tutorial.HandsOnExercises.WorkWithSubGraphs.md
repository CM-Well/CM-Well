# Hands-On Exercise: Work with Sub-Graphs #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.SubscribeForPulledData.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.AddFileInfoton.md)  

----


## Step Outline ##

1. [Add Quad Values](#hdr1).
2. [Replace Quad Values](#hdr2)
3. [Delete Quad Values](#hdr3).

<a name="hdr1"></a>
## 1. Add Quad Values ##

**Action:** Add 3 movie infotons, with critic scores from Rotten Tomatoes and New York Times.

**Curl command:**

    curl -X POST "<cm-well-host>/_in?format=nquads" --data-binary @input.txt

**File Contents:**

    <http://example/movies/ET> <http://www.w3.org/2000/01/rdf-schema#type> <http://dbpedia.org/ontology/Film>.
    <http://example/movies/GoneWithTheWind> <http://www.w3.org/2000/01/rdf-schema#type> <http://dbpedia.org/ontology/Film>.
    <http://example/movies/TheAvenger> <http://www.w3.org/2000/01/rdf-schema#type> <http://dbpedia.org/ontology/Film>.
    <http://example/movies/ET> <http://MyOntology/Score> "8.3" <http://MyOntology/RottenTomatoes>.
    <http://example/movies/ET> <http://MyOntology/Score> "8.7" <http://MyOntology/NewYorkTimes>.
    <http://example/movies/GoneWithTheWind> <http://MyOntology/Score> "6.5" <http://MyOntology/RottenTomatoes>.
    <http://example/movies/GoneWithTheWind> <http://MyOntology/Score> "8.9" <http://MyOntology/NewYorkTimes>.
    <http://example/movies/TheAvenger> <http://MyOntology/Score> "7.2" <http://MyOntology/RottenTomatoes>.
    <http://example/movies/TheAvenger> <http://MyOntology/Score> "7.7" <http://MyOntology/NewYorkTimes>.
    
**Response:**

    {"success":true}
    
<a name="hdr2"></a>
## 2. Replace Quad Values ##

**Action:** Change all New York Times scores to 10.

**Curl command:**

    curl -X POST "<cm-well-host>/_in?format=nquads" --data-binary @input.txt

**File Contents:**

    <> <cmwell://meta/sys#replaceGraph> <http://MyOntology/NewYorkTimes>. 
    <http://example.org/movies/ET> <http://MyOntology/Score> "10" <http://MyOntology/NewYorkTimes>.
    <http://example.org/movies/GoneWithTheWind> <http://MyOntology/Score> "10" <http://MyOntology/NewYorkTimes>.
    <http://example.org/movies/TheAvenger> <http://MyOntology/Score> "10" <http://MyOntology/NewYorkTimes>.

**Response:**

    {"success":true}
   
<a name="hdr3"></a>
## 3. Delete Quad Values ##

**Action:** Delete all Rotten Tomatoes scores.

**Curl command:**

    curl -X POST "<cm-well-host>/_in?format=nquads" --data-binary @input.txt

**File Contents:**

    <http://example/movies/ET> <cmwell://meta/sys#markReplace> <*> <http://MyOntology/RottenTomatoes>.
    <http://example/movies/GoneWithTheWind> <cmwell://meta/sys#markReplace> <*> <http://MyOntology/RottenTomatoes>.
    <http://example/movies/TheAvenger> <cmwell://meta/sys#markReplace> <*> <http://MyOntology/RottenTomatoes>.
    
**Response:**

    {"success":true}

## API Reference ##
[Add Infotons and Fields to Sub-Graph](API.Update.AddInfotonsAndFieldsToSubGraph.md)
[Delete or Replace Values in Named Sub-Graph](API.Update.DeleteOrReplaceValuesInNamedSubGraph.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.SubscribeForPulledData.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.AddFileInfoton.md)  

----