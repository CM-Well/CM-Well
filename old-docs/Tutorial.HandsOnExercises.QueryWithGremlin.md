# Hands-On Exercise: Query with Gremlin #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.QueryWithSPARQL.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.StreamWithTheStreamOperation.md)  

----

## Step Outline ##

1. [Upload Data](#hdr1)
2. [Run Gremlin Query](#hdr2)

<a name="hdr1"></a>
## 1. Upload Data ##

**Action:** Upload some person infotons, with relationships among the individuals.

**Curl command:**

    curl -X POST "<cm-well-host>/_in?format=nquads" -H "Content-Type: text/plain" --data-binary @inputfile.txt

**Input file contents:**

    <http://example.org/Individuals/DaisyDuck> <http://purl.org/vocab/relationship/colleagueOf> <http://example.org/Individuals/BruceWayne> .
      <http://example.org/Individuals/DaisyDuck> <http://www.tr-lbd.com/bold#active> "false" .
      <http://example.org/Individuals/BruceWayne> <http://purl.org/vocab/relationship/employedBy> <http://example.org/Individuals/DonaldDuck> .
      <http://example.org/Individuals/BruceWayne> <http://www.tr-lbd.com/bold#active> "true" .
      <http://example.org/Individuals/DonaldDuck> <http://purl.org/vocab/relationship/mentorOf> <http://example.org/Individuals/JohnSmith> .
      <http://example.org/Individuals/DonaldDuck> <http://purl.org/vocab/relationship/knowsByReputation> <http://example.org/Individuals/MartinOdersky> .
      <http://example.org/Individuals/DonaldDuck> <http://www.tr-lbd.com/bold#active> "true" .
      <http://example.org/Individuals/JohnSmith> <http://purl.org/vocab/relationship/friendOf> <http://example.org/Individuals/PeterParker> <http://example.org/graphs/spiderman> .
      <http://example.org/Individuals/JohnSmith> <http://purl.org/vocab/relationship/parentOf> <http://example.org/Individuals/SaraSmith> <http://example.org/graphs/spiderman> .
      <http://example.org/Individuals/JohnSmith> <http://www.tr-lbd.com/bold#active> "true" .
      <http://example.org/Individuals/SaraSmith> <http://purl.org/vocab/relationship/siblingOf> <http://example.org/Individuals/RebbecaSmith> .
      <http://example.org/Individuals/SaraSmith> <http://purl.org/vocab/relationship/childOf> <http://example.org/Individuals/HarryMiller> .
      <http://example.org/Individuals/SaraSmith> <http://www.tr-lbd.com/bold#active> "true" .
      <http://example.org/Individuals/RebbecaSmith> <http://purl.org/vocab/relationship/siblingOf> <http://example.org/Individuals/SaraSmith> .
      <http://example.org/Individuals/RebbecaSmith> <http://www.tr-lbd.com/bold#active> "false" .
      <http://example.org/Individuals/PeterParker> <http://purl.org/vocab/relationship/worksWith> <http://example.org/Individuals/HarryMiller> .
      <http://example.org/Individuals/PeterParker> <http://purl.org/vocab/relationship/neighborOf> <http://example.org/Individuals/ClarkKent> .
      <http://example.org/Individuals/PeterParker> <http://www.tr-lbd.com/bold#active> "true" .
      <http://example.org/Individuals/HarryMiller> <http://purl.org/vocab/relationship/parentOf> <http://example.org/Individuals/NatalieMiller> .
      <http://example.org/Individuals/HarryMiller> <http://www.tr-lbd.com/bold#active> "true" .
      <http://example.org/Individuals/NatalieMiller> <http://purl.org/vocab/relationship/childOf> <http://example.org/Individuals/HarryMiller> .
      <http://example.org/Individuals/NatalieMiller> <http://www.tr-lbd.com/bold#active> "true" .
      <http://example.org/Individuals/MartinOdersky> <http://purl.org/vocab/relationship/collaboratesWith> <http://example.org/Individuals/RonaldKhun> .
      <http://example.org/Individuals/MartinOdersky> <http://www.tr-lbd.com/bold#active> "true" .
      <http://example.org/Individuals/RonaldKhun> <http://purl.org/vocab/relationship/collaboratesWith> <http://example.org/Individuals/MartinOdersky> .
      <http://example.org/Individuals/RonaldKhun> <http://www.tr-lbd.com/bold#active> "true" .
      <http://example.org/Individuals/RonaldKhun> <http://www.tr-lbd.com/bold#category> "deals" .
      <http://example.org/Individuals/RonaldKhun> <http://www.tr-lbd.com/bold#category> "news" .

**Response:**

	{"success":true}

<a name="hdr2"></a>
## 2. Run Gremlin Query ##

**Action:** Retrieve person infotons which are outbound links of the "Sara Smith" infoton, whose **active** field value is "true".

**Curl command:**

    curl -X POST "<cm-well-host>/_sp" --data-binary @inputfile.txt

**Input file contents:**

    PATHS
    /example.org/Individuals?op=search&length=1000&with-data
    
    Gremlin
    g.v("http://example.org/Individuals/SaraSmith").out().filter{it["http://www.tr-lbd.com/bold#active"]=="true"}


**Response:**

	v[http://example.org/Individuals/RebbecaSmith]
                
## API Reference ##
[Apply Gremlin to Query Results](API.Query.ApplyGremlinToQueryResults.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.QueryWithSPARQL.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.StreamWithTheStreamOperation.md)  

----