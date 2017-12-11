# Hands-On Exercise: Subscribe for Pulled Data #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.StreamWithIterator.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.WorkWithSubGraphs.md)  

----

## Step Outline ##

1. [Subscribe for Pulled Data](#hdr1).
2. [Get Latest Data](#hdr2)
3. [Unsubscribe](#hdr3).

<a name="hdr1"></a>
## 1. Query by One Field Value ##

**Action:** Get all new entities under permid.org.

**Curl command:**

    curl "<cm-well-host>/permid.org?op=subscribe&method=pull"

**Response:**

    YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjczLjE2MDozNDA5NS91c2VyLzdlYWM2NzI3
    
<a name="hdr2"></a>
## 2. Get Latest Data ##

**Action:** Get the latest updated infotons under permid.org.

**Curl command:**

    curl "<cm-well-host>/permid.org?op=pull&sub=YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjczLjE2MDozNDA5NS91c2VyLzdlYWM2NzI3&format=text"

**Response:**

    	o:nRSR4409Ea-2016-07-18
        sys:data                       "<Document>\n<Source>RNS</Source>..." ;
        sys:dataCenter                 "dc1" ;
        sys:indexTime                  "1468833593106"^^xsd:long ;
        sys:lastModified               "2016-07-18T09:19:51.344Z"^^xsd:dateTime ;
        sys:length                     "11981"^^xsd:long ;
        sys:mimeType                   "text/plain; utf-8" ;
        sys:parent                     "/data.com/sc/docs/input/news" ;
        sys:path                       "/data.com/sc/docs/input/news/nRSR4409Ea-2016-07-18" ;
        sys:type                       "FileInfoton" ;
        sys:uuid                       "05e3ae867c2928cb7cd2e8ec254bf005" ;
        supplyChain:Codes              "LSEN" ;
        supplyChain:DATE               "2016-07-18"^^xsd:dateTime ;
        supplyChain:Feed               "UCDP" ;
        supplyChain:MetaCodes.product  "LSEN" ;
        supplyChain:MetaCodes.rcscode  "M:1NN" , "B:201" , "R:DRTY.L" , "G:AL" , "G:3" , "M:Z" , "R:FNAC.PA" , "P:4295897654" , "B:100" , "G:7J" , "M:32" , "B:202" , "B:69" , "M:3H" , "G:5M" , "P:4295867209" , "B:104" , "G:A" , "B:98" ;
        supplyChain:PNAC               "nRSR4409Ea" ;
        supplyChain:Source             "RNS" ;
        supplyChain:Title              "REG - Morgan Stanley & Co. Darty PLC Groupe FNAC - Form 8.5 (EPT/RI)Groupe Fnac SA" ;
        supplyChain:Urgency            "3"^^xsd:int .
   
<a name="hdr3"></a>
## 3. Unsubscribe ##

**Action:** Unsubscribe from real-time updates.

**Curl command:**

    curl "<cm-well-host>/permid.org?op=unsubscribe&sub=YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjczLjE2MDozNDA5NS91c2VyLzdlYWM2NzI3"

**Response:**

    unsubscribe YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjczLjE2MDozNDA5NS91c2VyLzdlYWM2NzI3

## API Reference ##
[Subscribe for Pulled Data](API.Subscribe.SubscribeForPulledData.md)
[Pull New Data](API.Subscribe.PullNewData.md)
[Unsubscribe](API.Subscribe.Unsubscribe.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.StreamWithIterator.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.WorkWithSubGraphs.md)  

----
