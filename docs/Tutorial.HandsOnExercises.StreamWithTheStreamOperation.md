# Hands-On Exercise: Stream with the Stream Operation #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.QueryWithGremlin.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.StreamWithIterator.md)  

----

**Action:** Stream all infotons whose type is Organization from the permid.org path in CM-Well. Display only their CommonName triples. You can run this command in a command-line window. Streaming will continue until you abort the operation (e.g. using ctrl-C).

**Curl command:**

    curl "<cm-well-host>/permid.org?op=stream&format=ntriples&fields=CommonName.mdaas&qp=type.rdf::http://ont.thomsonreuters.com/mdaas/Organization"

**Response: (truncated)**

    <http://permid.org/1-5037249647> <http://ont.thomsonreuters.com/mdaas/CommonName> "Sports Field Holdings Inc" .
    <http://permid.org/1-5040272935> <http://ont.thomsonreuters.com/mdaas/CommonName> "LOGIC INVEST, A.S." .
    <http://permid.org/1-4296216818> <http://ont.thomsonreuters.com/mdaas/CommonName> "VALUE CONTRARIAN CANADIAN EQUITY FUND" .
    <http://permid.org/1-4296057952> <http://ont.thomsonreuters.com/mdaas/CommonName> "UNIBANCORP INC-COMMERCIAL FIN" .
    <http://permid.org/1-5045164267> <http://ont.thomsonreuters.com/mdaas/CommonName> "Trans Winaton NV" .
    <http://permid.org/1-4296004718> <http://ont.thomsonreuters.com/mdaas/CommonName> "Residential Delivery Services Inc" .
    <http://permid.org/1-5044799944> <http://ont.thomsonreuters.com/mdaas/CommonName> "Servipark Peiate SL" .
    <http://permid.org/1-5000651786> <http://ont.thomsonreuters.com/mdaas/CommonName> "Proven Organics" .
    <http://permid.org/1-5043378395> <http://ont.thomsonreuters.com/mdaas/CommonName> "Evenstone Pty Ltd" .
    <http://permid.org/1-5046708627> <http://ont.thomsonreuters.com/mdaas/CommonName> "China Tobacco Corp Beijing Co" .
    <http://permid.org/1-5000100797> <http://ont.thomsonreuters.com/mdaas/CommonName> "Portoline OU" .
    <http://permid.org/1-4298380507> <http://ont.thomsonreuters.com/mdaas/CommonName> "Astreya OOO" .
    <http://permid.org/1-5001085989> <http://ont.thomsonreuters.com/mdaas/CommonName> "County Agencies" .
    <http://permid.org/1-4297236436> <http://ont.thomsonreuters.com/mdaas/CommonName> "Al Tameer Real Estate Co" .
    <http://permid.org/1-5044261953> <http://ont.thomsonreuters.com/mdaas/CommonName> "Hainesville Village-Illinois" .
    <http://permid.org/1-5044835168> <http://ont.thomsonreuters.com/mdaas/CommonName> "Belemnit Ooo" .
    <http://permid.org/1-5044636240> <http://ont.thomsonreuters.com/mdaas/CommonName> "Bunka Kensetsu KK" .
    <http://permid.org/1-5038078670> <http://ont.thomsonreuters.com/mdaas/CommonName> "Moelven Wood Prosjekt As"@en .
    <http://permid.org/1-4298027942> <http://ont.thomsonreuters.com/mdaas/CommonName> "Kanachu Kanko Co Ltd" .
    <http://permid.org/1-5017190875> <http://ont.thomsonreuters.com/mdaas/CommonName> "RJD Contractors Inc" .
    <http://permid.org/1-4296037972> <http://ont.thomsonreuters.com/mdaas/CommonName> "M&R MARTINI & ROSSI LTD" .
    <http://permid.org/1-4296248337> <http://ont.thomsonreuters.com/mdaas/CommonName> "Planters Equipment Co Inc" .
    <http://permid.org/1-5000610829> <http://ont.thomsonreuters.com/mdaas/CommonName> "Municipalite Saint Wenceslas" .
    <http://permid.org/1-5035558956> <http://ont.thomsonreuters.com/mdaas/CommonName> "Teldent Ltd" .
    <http://permid.org/1-5001071578> <http://ont.thomsonreuters.com/mdaas/CommonName> "Palmer Davis Seika Inc" .
    <http://permid.org/1-5042239591> <http://ont.thomsonreuters.com/mdaas/CommonName> "Fujiki Corporation KK" .
    <http://permid.org/1-4296666350> <http://ont.thomsonreuters.com/mdaas/CommonName> "Champion Intl Brown Paper Sys" .
    <http://permid.org/1-5044190752> <http://ont.thomsonreuters.com/mdaas/CommonName> "HFP Holdings Inc" .
    <http://permid.org/1-4296686895> <http://ont.thomsonreuters.com/mdaas/CommonName> "Carrot's Ink Cartridges" .
    <http://permid.org/1-5014129143> <http://ont.thomsonreuters.com/mdaas/CommonName> "687336 Ontario Ltd" .
    <http://permid.org/1-5001124463> <http://ont.thomsonreuters.com/mdaas/CommonName> "Ppm 2000 Inc" .
    <http://permid.org/1-4296856642> <http://ont.thomsonreuters.com/mdaas/CommonName> "AGRO-PECUANIA CFM LTDA" .
    <http://permid.org/1-5037911314> <http://ont.thomsonreuters.com/mdaas/CommonName> "Thunder Production Services Inc" .
    <http://permid.org/1-5001311345> <http://ont.thomsonreuters.com/mdaas/CommonName> "Hoyukai Medical Association" .
    <http://permid.org/1-5036211782> <http://ont.thomsonreuters.com/mdaas/CommonName> "Fairdeal Textile Park Pvt Ltd" .
    <http://permid.org/1-4296277224> <http://ont.thomsonreuters.com/mdaas/CommonName> "B2sb Technologies Corp" .
    <http://permid.org/1-5035702933> <http://ont.thomsonreuters.com/mdaas/CommonName> "GNMA REMIC Trust 1996-14" .
    <http://permid.org/1-4296789461> <http://ont.thomsonreuters.com/mdaas/CommonName> "BROMAR INC" .
    <http://permid.org/1-5041178494> <http://ont.thomsonreuters.com/mdaas/CommonName> "R K Water Truck Rental Inc" .
    <http://permid.org/1-4296711536> <http://ont.thomsonreuters.com/mdaas/CommonName> "1500 Records" .
    <http://permid.org/1-5045858458> <http://ont.thomsonreuters.com/mdaas/CommonName> "Guggenheim Defined Portfolios Series 753" .
    
    ...
        
## API Reference ##
[Stream Infotons](API.Stream.StreamInfotons.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.QueryWithGremlin.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.StreamWithIterator.md)  

----