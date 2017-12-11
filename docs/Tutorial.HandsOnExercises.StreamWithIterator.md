# Hands-On Exercise: Stream with Iterator #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.StreamWithTheStreamOperation.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.SubscribeForPulledData.md)  

----

## Step Outline ##

1. [Create Iterator](#hdr1).
2. [Get Next Chunk](#hdr2)

<a name="hdr1"></a>
## 1. Create Iterator ##

**Action:** Create an iterator for retrieving all infotons under permid.org, with a session timeout of 60 seconds.

**Curl command:**

    Curl "<cm-well-host>/permid.org?op=create-iterator&session-ttl=60000"

**Response:**

`{"type":"IterationResults","iteratorId":"YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjE3Ny4xOjQ4ODMxL3VzZXIvJGJkYiMtMTc4MTcxOTQwMA","totalHits":94271748,"infotons":[]}`    
    
<a name="hdr2"></a>
## 2. Get Next Chunk ##

**Action:** Get the next chunk of infotons.

**Curl command:**

    Curl "<cm-well-host>/permid.org?op=next-chunk&format=text&iterator-id=YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjE3Ny4xOjQ4ODMxL3VzZXIvJGJkYiMtMTc4MTcxOTQwMA"

**Response: (truncated)**

    /permid.org/1-21568482685
    /permid.org/1-21580881855
    /permid.org/1-21581865726
    /permid.org/1-21575060029
    /permid.org/1-21582269193
    /permid.org/1-21591345657
    /permid.org/1-21488552284
    /permid.org/1-21521516658
    /permid.org/1-21591870173
    /permid.org/1-21547405276
    /permid.org/1-5013071490
    /permid.org/1-21575672989
    /permid.org/1-21580881956
    /permid.org/1-21581873542
    /permid.org/1-21520024307
    /permid.org/1-21579664297
    /permid.org/1-5051390958
    /permid.org/1-21488618092
    /permid.org/1-21523206626
    /permid.org/1-21591885498
    /permid.org/1-21501231546
    /permid.org/1-5035946005
    /permid.org/1-21580881913
    /permid.org/1-21579628021
    /permid.org/1-21582001600
    /permid.org/1-5035157347
    /permid.org/1-21590887958
    /permid.org/1-21478191061
    /permid.org/1-21508882736
    /permid.org/1-18068376296
    /permid.org/1-4296477614
    /permid.org/1-5035890025
    /permid.org/1-21580882088
    /permid.org/1-21581889824
    /permid.org/1-21582110667
    /permid.org/1-21583763573
    /permid.org/1-21589697989
    /permid.org/1-21488094136
    /permid.org/1-21482237965
    /permid.org/1-21591877425
    /permid.org/1-44636899891
    /permid.org/1-5000452316
    /permid.org/1-5048596216
    /permid.org/1-21580882074
    /permid.org/1-21581892706
    /permid.org/1-21528445248
    /permid.org/1-21525076756
    /permid.org/1-5000219021
    /permid.org/1-21488613002
    /permid.org/1-21558246353
    /permid.org/1-21591613531
    /permid.org/1-5046060305
    /permid.org/1-21489652020
    /permid.org/1-21521413252
    /permid.org/1-21581865725
    /permid.org/1-21574480806
    /permid.org/1-21582275031
    /permid.org/1-21591343214
    /permid.org/1-21487993355
    /permid.org/1-21559482266
    /permid.org/1-21591909662
    /permid.org/1-21563832851
    /permid.org/1-5049096098
    /permid.org/1-21580881937
    /permid.org/1-21581806260
    /permid.org/1-21556465288
    /permid.org/1-21583830902
    /permid.org/1-21529570723
    /permid.org/1-21488608017
    /permid.org/1-21539740273
    /permid.org/1-21591871192
    /permid.org/1-21565873982
    /permid.org/1-21570267509
    /permid.org/1-5042243009
    /permid.org/1-21581883351
    /permid.org/1-21556474777
    /permid.org/1-21579668412
    /permid.org/1-21590881113
    /permid.org/1-21488450878
    /permid.org/1-21492520856
    /permid.org/1-8590619498
    /permid.org/1-21565338620
    /permid.org/1-5000640649
    /permid.org/1-21580881859
    /permid.org/1-5000402082
    /permid.org/1-21576588844
    /permid.org/1-21512480306
    /permid.org/1-21591379116
    /permid.org/1-21488368741
    /permid.org/1-21558983931
    /permid.org/1-21523298659
    /permid.org/1-4298179818
    /permid.org/1-5000183475
    /permid.org/1-21580881854
    /permid.org/1-21549411863
    /permid.org/1-21574852957
    /permid.org/1-18068275065
    /permid.org/1-21591352027
    /permid.org/1-21487990515
    /permid.org/1-21474872424
    /permid.org/1-21579984601
    /permid.org/1-21548845859
    /permid.org/1-5043332094
    /permid.org/1-21580881852
    /permid.org/1-5050279707
    /permid.org/1-21581993283
    /permid.org/1-21516558266
    /permid.org/1-21478088166
    /permid.org/1-30064856600
    /permid.org/1-21526646995
    /permid.org/1-21591877885
    /permid.org/1-21479763936
    /permid.org/1-21566307920
    /permid.org/1-5035313711
    /permid.org/1-55839467139
    /permid.org/1-21581875322
    /permid.org/1-21574479411
    /permid.org/1-21544147689
    /permid.org/1-21591286646
    /permid.org/1-21479617467
    /permid.org/1-55870425147
    /permid.org/1-21577738417
    /permid.org/1-21552803529
    /permid.org/1-5037932339
    /permid.org/1-21580881857
    /permid.org/1-21581875332
    /permid.org/1-21524902390
    /permid.org/1-21583533143
    /permid.org/1-5051388402
    /permid.org/1-4295956437
    /permid.org/1-21559070814
    /permid.org/1-21591877662
    /permid.org/1-21561636345
    /permid.org/1-21485759451
    /permid.org/1-21563714085
    /permid.org/1-21580882143
    /permid.org/1-21574992466
    /permid.org/1-21562569020
    /permid.org/1-21571639830
    /permid.org/1-21590290302
    /permid.org/1-21488388271
    /permid.org/1-21532662471
    /permid.org/1-21591871530
    /permid.org/1-5000938143
    /permid.org/1-21564820233
    /permid.org/1-5046292233
    /permid.org/1-21580881850
    /permid.org/1-5000019104
    /permid.org/1-21581654770
    /permid.org/1-21582772365
    /permid.org/1-21478088320
    /permid.org/1-21476453443
    /permid.org/1-21484813520
    /permid.org/1-21591872247
    /permid.org/1-21495136489
    /permid.org/1-21579658507
    /permid.org/1-21580881864
    /permid.org/1-21580789368
    /permid.org/1-5000061310
    /permid.org/1-21582482466
    /permid.org/1-21591341349
    /permid.org/1-21483344815
    /permid.org/1-21559168048
    /permid.org/1-21591913977
    /permid.org/1-4296791421
    /permid.org/1-21576341202
    /permid.org/1-21580881919
    /permid.org/1-21581784427
    /permid.org/1-21578836421
    /permid.org/1-5045675255
    /permid.org/1-21591345948
    /permid.org/1-21591908756
    /permid.org/1-21591340756
    /permid.org/1-21591871059
    /permid.org/1-21591339811
    /permid.org/1-21591876194
    /permid.org/1-21558320602
    /permid.org/1-21591901306
    /permid.org/1-5039681949
    /permid.org/1-21514426908
    /permid.org/1-21591349673
    /permid.org/1-21591908176
    /permid.org/1-8589966963
    /permid.org/1-21591876592
    /permid.org/1-21591355648
    /permid.org/1-21591870313
    /permid.org/1-21591346066
    /permid.org/1-21475048627
    /permid.org/1-21591340780
    /permid.org/1-21591876949
    /permid.org/1-21591354789
    /permid.org/1-21591872368
    /permid.org/1-21590512357
    /permid.org/1-21514427380
    /permid.org/1-21591349692
    /permid.org/1-21591873320
    /permid.org/1-21591353604
    /permid.org/1-21591914195
    /permid.org/1-21591349806
    /permid.org/1-5037119808
    /permid.org/1-5035747133
    /permid.org/1-21591287581
    /permid.org/1-21478089114
    /permid.org/1-47034259272
    /permid.org/1-21591339705
    /permid.org/1-21591866294
    /permid.org/1-21589296852
    /permid.org/1-21586852710
    /permid.org/1-21591284610
    /permid.org/1-21591885796
    /permid.org/1-21478088773
    /permid.org/1-21591908757
    /permid.org/1-21590492363
    /permid.org/1-21591909367
    /permid.org/1-21591341410
    /permid.org/1-21591883341
    /permid.org/1-21591339888
    /permid.org/1-21591877787

## API Reference ##
[Create Iterator](API.Stream.CreateIterator.md)
[Get Next Chunk](API.Stream.GetNextChunk.md)
       
----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.HandsOnExercisesTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercises.StreamWithTheStreamOperation.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tutorial.HandsOnExercises.SubscribeForPulledData.md)  

----