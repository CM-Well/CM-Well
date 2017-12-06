# Hands-On Exercise: Delete Infotons and Fields #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.UpdateFields.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.QueryForInfotons.md)  

----

## Step Outline ##

1. [Add Infotons and Fields](#hdr1).
2. [Delete Infotons and Fields](#hdr2)

<a name="hdr1"></a>
## 1. Add Infotons and Fields ##

**Action:** Add several infotons and field values. 

**Curl command:**

    curl -X POST "<cm-well-host>/_in?format=ntriples" --data-binary @input.txt

**File contents:**

    <http://example/Individuals/MamaBear> <http://purl.org/vocab/relationship/spouseOf> <http://example/Individuals/PapaBear> .
    <http://example/Individuals/MamaBear> <http://ont.thomsonreuters.com/bermuda/hasName> "Betty".
    <http://example/Individuals/PapaBear> <http://purl.org/vocab/relationship/spouseOf> <http://example/Individuals/MamaBear> .
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

**Action:** Delete all "BabyBear" infotons and all name fields.
**Curl command:**

    curl -X POST "<cm-well-host>/_in?format=ntriples" --data-binary @input.txt

**File contents:**

    <http://example/Individuals/BabyBear1> <cmwell://meta/sys#fullDelete> "false" .
    <http://example/Individuals/BabyBear2> <cmwell://meta/sys#fullDelete> "false" .
    <http://example/Individuals/BabyBear3> <cmwell://meta/sys#fullDelete> "false" .
    <http://example/Individuals/MamaBear> <cmwell://meta/sys#markReplace> <http://ont.thomsonreuters.com/bermuda/hasName> .
    <http://example/Individuals/PapaBear> <cmwell://meta/sys#markReplace> <http://ont.thomsonreuters.com/bermuda/hasName> .

**Response:**

    {"success":true}

## API Reference ##
[Add Infotons and Fields](API.Update.AddInfotonsAndFields.md)
[Delete Multiple Infotons](API.Update.DeleteMultipleInfotons.md)
[Delete Fields](API.Update.DeleteFields.md)
    	

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.UpdateFields.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.QueryForInfotons.md)  

----