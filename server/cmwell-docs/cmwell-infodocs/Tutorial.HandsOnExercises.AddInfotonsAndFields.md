# Hands-On Exercise: Add Infotons and Fields #

**Action:** Add several infotons and field values. After adding them, you can browse to their URIs and examine their fields.

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

## API Reference ##
[Add Infotons and Fields](API.Update.AddInfotonsAndFields.md)
    
