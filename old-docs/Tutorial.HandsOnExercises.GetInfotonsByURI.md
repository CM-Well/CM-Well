# Hands-On Exercise: Get Infotons by URI #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercisesTOC.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.AddInfotonsAndFields.md)  

----

## Step Outline ##

1. [Upload infotons](#hdr1).
2. [Get a single infoton by URI](#hdr2).
3. [Get multiple infotons by URI](#hdr3).

<a name="hdr1"></a>
## 1. Upload Infotons ##

**Action:** Upload triples for 2 person entities with gender and name fields.

**Curl command:**

    curl -X POST "<cm-well-host>/_in?format=ntriples" -H "Content-Type: text/plain" --data-binary @inputfile.txt

**Input file contents:**

    <http://exercise/JohnSmith> <http://www.w3.org/2006/vcard/ns#GENDER> "Male" .
    <http://exercise/JohnSmith> <http://www.w3.org/2006/vcard/ns#FN> "John Smith" .
    <http://exercise/JohnSmith> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Individual> .
    <http://exercise/JaneSmith> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Individual> .
    <http://exercise/JaneSmith> <http://www.w3.org/2006/vcard/ns#GENDER> "Female" .
    <http://exercise/JaneSmith> <http://www.w3.org/2006/vcard/ns#FN> "Jane Smith" .

**Response:**

    {"success":true}

<a name="hdr2"></a>
## 2. Get a Single Infoton by URI ##

**Action:** Get "Jane Smith" infoton.

**Curl command:**

    curl "<cm-well-host>/exercise/JaneSmith?format=json&pretty"

**Response:**

    {
      "type" : "ObjectInfoton",
      "system" : {
    	"uuid" : "8efaa16a187376ea6663b1882e371d5d",
    	"lastModified" : "2016-07-08T15:20:12.496Z",
    	"path" : "/exercise/JaneSmith",
    	"dataCenter" : "dc1",
    	"indexTime" : 1467991213647,
    	"parent" : "/exercise"
      },
      "fields" : {
    	"FN.vcard" : [ "Jane Smith" ],
    	"GENDER.vcard" : [ "Female" ],
    	"type.rdf" : [ "http://www.w3.org/2006/vcard/ns#Individual" ]
      }
    }
   
<a name="hdr3"></a>
## 3. Get Multiple Infotons by URI ##

**Action:** Get "John Smith" and "Jane Smith" infotons.

**Curl command:**

    curl -X POST "<cm-well-host>/_out?format=json&pretty" -H "Content-Type: text/plain" --data-binary @inputfile.txt

    
**Input file contents:**

    /exercise/JohnSmith
    /exercise/JaneSmith

**Response:**

    {  
     "type":"RetrievablePaths",
     "infotons":[  
        {  
         "type":"ObjectInfoton",
         "system":{  
            "uuid":"181063461a14dae633f096ff4d142f00",
            "lastModified":"2016-07-08T15:20:12.496Z",
            "path":"/exercise/JohnSmith",
            "dataCenter":"dc1",
            "indexTime":1467991213647,
            "parent":"/exercise"
         },
         "fields":{  
            "FN.vcard":[  
               "John Smith"
            ],
            "GENDER.vcard":[  
               "Male"
            ],
            "type.rdf":[  
               "http://www.w3.org/2006/vcard/ns#Individual"
            ]
         }
      },
      {  
         "type":"ObjectInfoton",
         "system":{  
            "uuid":"8efaa16a187376ea6663b1882e371d5d",
            "lastModified":"2016-07-08T15:20:12.496Z",
            "path":"/exercise/JaneSmith",
            "dataCenter":"dc1",
            "indexTime":1467991213647,
            "parent":"/exercise"
         },
         "fields":{  
            "FN.vcard":[  
               "Jane Smith"
            ],
            "GENDER.vcard":[  
               "Female"
            ],
            "type.rdf":[  
               "http://www.w3.org/2006/vcard/ns#Individual"
            ]
         }
      }
     ],
     "irretrievablePaths":[]
    }    

## API Reference ##
[Add Infotons and Fields](API.Update.AddInfotonsAndFields.md)
[Get a Single Infoton by URI](API.Get.GetSingleInfotonByURI.md)
[Get Multiple Infotons by URI](API.Get.GetMultipleInfotonsByURI.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercisesTOC.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.AddInfotonsAndFields.md)  

----