# Hands-On Exercise: Update Fields #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.AddInfotonsAndFields.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.DeleteInfotonsAndFields.md)  

----

## Step Outline ##

1. [Add Infotons and Fields](#hdr1).
2. [Update Field Values](#hdr2)

<a name="hdr1"></a>
## 1. Add Infotons and Fields ##

**Action:** Add several infotons and field values. 

**Curl command:**

    curl -X POST "<cm-well-host>/_in?format=ntriples" --data-binary @input.txt

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
## 2. Update Field Values ##

**Action:** Change values of all "BabyBear" name fields. After changing the field values, you can browse to the infoton URIs to see the updated values.

>**Note:** You can also update fields without replacing the existing value, by adding a new triple with the same predicate name as the existing field. In this case, two (or more) instances of the same field exist, with the same name but different values.

**Curl command:**

    curl -X POST "<cm-well-host>/_in?format=ntriples" --data-binary @input.txt

**File contents:**

    <http://example/Individuals/BabyBear1> <cmwell://meta/sys#markReplace> <http://ont.thomsonreuters.com/bermuda/hasName> . 
    <http://example/Individuals/BabyBear1> <http://ont.thomsonreuters.com/bermuda/hasName> "Cathy" .
    <http://example/Individuals/BabyBear2> <cmwell://meta/sys#markReplace> <http://ont.thomsonreuters.com/bermuda/hasName> . 
    <http://example/Individuals/BabyBear2> <http://ont.thomsonreuters.com/bermuda/hasName> "Craig" .
    <http://example/Individuals/BabyBear3> <cmwell://meta/sys#markReplace> <http://ont.thomsonreuters.com/bermuda/hasName> . 
    <http://example/Individuals/BabyBear3> <http://ont.thomsonreuters.com/bermuda/hasName> "Curt" .

**Response:**

    {"success":true}
    
## API Reference ##
[Add Infotons and Fields](API.Update.AddInfotonsAndFields.md)
[Replace Field Values](API.Update.ReplaceFieldValues.md)
    	

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.AddInfotonsAndFields.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.DeleteInfotonsAndFields.md)  

----