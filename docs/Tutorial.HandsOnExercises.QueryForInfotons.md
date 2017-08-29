# Hands-On Exercise: Query for Infotons #

## Step Outline ##

>**Note:** These steps are not dependent on each other and do not have to be completed in order.

1. [Query by One Field Value](#hdr1).
2. [Get Results with Data](#hdr2)
3. [Use Various Comparison Operators](#hdr3).
4. [Specify Output Fields](#hdr4).
5. [Implement AND, OR, NOT](#hdr5).
6. [Page through Results](#hdr6).
7. [Traverse Inbound and Outbound Links](#hdr7).

<a name="hdr1"></a>
## 1. Query by One Field Value ##

**Action:** Get up to 3 entities under permid.org, whose CommonName value contains the string "Reuters".

**Curl command:**

    curl "<cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Reuters&length=3&format=json&pretty"

**Response:**

    {
      "type" : "SearchResponse",
      "pagination" : {
    	"type" : "PaginationInfo",
    	"first" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A40%3A01.740Z&to=2016-07-08T15%3A40%3A01.979Z&qp=CommonName.mdaas%3AReuters&length=3&offset=0",
    	"self" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A40%3A01.740Z&to=2016-07-08T15%3A40%3A01.979Z&qp=CommonName.mdaas%3AReuters&length=3&offset=0",
    	"next" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A40%3A01.740Z&to=2016-07-08T15%3A40%3A01.979Z&qp=CommonName.mdaas%3AReuters&length=3&offset=3",
    	"last" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A40%3A01.740Z&to=2016-07-08T15%3A40%3A01.979Z&qp=CommonName.mdaas%3AReuters&length=3&offset=70914"
      },
      "results" : {
    	"type" : "SearchResults",
    	"fromDate" : "2016-07-08T15:40:01.740Z",
    	"toDate" : "2016-07-08T15:40:01.979Z",
    	"total" : 70914,
    	"offset" : 0,
    	"length" : 3,
    "infotons" : [ {
      "type" : "ObjectInfoton",
      "system" : {
    	"uuid" : "08e369b8d4660826eb9494810e0af09b",
    	"lastModified" : "2016-07-08T15:40:01.979Z",
    	"path" : "/permid.org/1-21591377740",
    	"dataCenter" : "dc1",
    	"indexTime" : 1467992402553,
    	"parent" : "/permid.org"
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
    	"uuid" : "c8fc5e38b51892b529caa66646d6a56a",
    	"lastModified" : "2016-07-08T15:40:01.940Z",
    	"path" : "/permid.org/1-21591377619",
    	"dataCenter" : "dc1",
    	"indexTime" : 1467992403912,
    	"parent" : "/permid.org"
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
    	"uuid" : "f188963093cd7b90a169c81bb8973daf",
    	"lastModified" : "2016-07-08T15:40:01.740Z",
    	"path" : "/permid.org/1-21591374238",
    	"dataCenter" : "dc1",
    	"indexTime" : 1467992402499,
    	"parent" : "/permid.org"
      }
    } ]
      }
    }
    
<a name="hdr2"></a>
## 2. Get Results with Data ##

**Action:** Get up to 3 entities under permid.org, whose CommonName value contains the string "Reuters", with their data, i.e. with all fields and not just system fields.

**Curl command:**

    curl "<cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Reuters&length=3&format=json&pretty&with-data"

**Response:**

    {
      "type" : "SearchResponse",
      "pagination" : {
    "type" : "PaginationInfo",
    "first" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A40%3A01.740Z&to=2016-07-08T15%3A40%3A01.979Z&qp=CommonName.mdaas%3AReuters&length=3&offset=0",
    "self" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A40%3A01.740Z&to=2016-07-08T15%3A40%3A01.979Z&qp=CommonName.mdaas%3AReuters&length=3&offset=0",
    "next" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A40%3A01.740Z&to=2016-07-08T15%3A40%3A01.979Z&qp=CommonName.mdaas%3AReuters&length=3&offset=3",
    "last" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A40%3A01.740Z&to=2016-07-08T15%3A40%3A01.979Z&qp=CommonName.mdaas%3AReuters&length=3&offset=70914"
      },
      "results" : {
        "type" : "SearchResults",
    "fromDate" : "2016-07-08T15:40:01.740Z",
    "toDate" : "2016-07-08T15:40:01.979Z",
    "total" : 70914,
    "offset" : 0,
    "length" : 3,
    "infotons" : [ {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "08e369b8d4660826eb9494810e0af09b",
        "lastModified" : "2016-07-08T15:40:01.979Z",
        "path" : "/permid.org/1-21591377740",
        "dataCenter" : "dc1",
        "indexTime" : 1467992402553,
        "parent" : "/permid.org"
      },
      "fields" : {
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Instrument" ],
        "instrumentExpiryDate.mdaas" : [ "2050-12-31" ],
        "isIssuedBy.mdaas" : [ "http://permid.org/1-4296733007" ],
        "CommonName.mdaas" : [ "Goldman Sachs & Co Wertpapier GMBH Call 1313.7255 USD Thomson Reuters: Gold / US Dollar FX Spot Rate 31Dec99" ],
        "instrumentCurrencyName.mdaas" : [ "EUR" ],
        "instrumentStatus.mdaas" : [ "Active" ],
        "RCSAssetClass.mdaas" : [ "BARWNT" ],
        "instrumentAssetClass.mdaas" : [ "Barrier Warrants" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "c8fc5e38b51892b529caa66646d6a56a",
        "lastModified" : "2016-07-08T15:40:01.940Z",
        "path" : "/permid.org/1-21591377619",
        "dataCenter" : "dc1",
        "indexTime" : 1467992403912,
        "parent" : "/permid.org"
      },
      "fields" : {
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Instrument" ],
        "instrumentExpiryDate.mdaas" : [ "2050-12-31" ],
        "isIssuedBy.mdaas" : [ "http://permid.org/1-4296733007" ],
        "CommonName.mdaas" : [ "Goldman Sachs & Co Wertpapier GMBH Call 1240 USD Thomson Reuters: Gold / US Dollar FX Spot Rate 31Dec99" ],
        "instrumentCurrencyName.mdaas" : [ "EUR" ],
        "instrumentStatus.mdaas" : [ "Active" ],
        "RCSAssetClass.mdaas" : [ "BARWNT" ],
        "instrumentAssetClass.mdaas" : [ "Barrier Warrants" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "f188963093cd7b90a169c81bb8973daf",
        "lastModified" : "2016-07-08T15:40:01.740Z",
        "path" : "/permid.org/1-21591374238",
        "dataCenter" : "dc1",
        "indexTime" : 1467992402499,
        "parent" : "/permid.org"
      },
      "fields" : {
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Instrument" ],
        "instrumentExpiryDate.mdaas" : [ "2016-12-13" ],
        "isIssuedBy.mdaas" : [ "http://permid.org/1-8589934314" ],
        "CommonName.mdaas" : [ "COMMERZBANK AG Put 0.0001 USD Thomson Reuters: Gold / US Dollar FX Spot Rate 13Dec16" ],
        "instrumentCurrencyName.mdaas" : [ "EUR" ],
        "instrumentStatus.mdaas" : [ "Active" ],
        "RCSAssetClass.mdaas" : [ "BARWNT" ],
        "instrumentAssetClass.mdaas" : [ "Barrier Warrants" ]
      }
    } ]
      }
    } 
   
<a name="hdr3"></a>
## 3. Use Various Comparison Operators ##

### Partial Match ###

**Action:** Get all entities under permid.org, whose CommonName value contains the string "qqqqq" (there are no infotons that match this query). The field condition uses the `:` partial match operator.

**Partial match Curl command:**    

    curl "<cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:qqqqq&format=json&pretty&with-data"

**Partial match response:**

    {
      "type" : "SearchResponse",
      "pagination" : {
	    "type" : "PaginationInfo",
	    "first" : "http://cm-well-host.com/permid.org?format=json?&op=search&qp=CommonName.mdaas%3Aqqqqq&length=0&offset=0",
	    "previous" : "http://cm-well-host.com/permid.org?format=json?&op=search&qp=CommonName.mdaas%3Aqqqqq&length=0&offset=0",
	    "self" : "http://cm-well-host.com/permid.org?format=json?&op=search&qp=CommonName.mdaas%3Aqqqqq&length=0&offset=0",
	    "last" : "http://cm-well-host.com/permid.org?format=json?&op=search&qp=CommonName.mdaas%3Aqqqqq&length=0&offset=0"
      },
      "results" : {
	    "type" : "SearchResults",
	    "total" : 0,
	    "offset" : 0,
	    "length" : 0,
	    "infotons" : [ ]
      }
    }

### Fuzzy Match ###

**Action:** Get all entities under permid.org, whose CommonName value is a "fuzzy" (approximate) match of the string "qqqqq". The field condition uses the `~` fuzzy match operator.

**Fuzzy match Curl command:**    

    curl "<cm-well-host>/permid.org?op=search&qp=CommonName.mdaas~qqqqq&format=json&pretty&with-data"

**Fuzzy match response:**

    {
      "type" : "SearchResponse",
      "pagination" : {
    "type" : "PaginationInfo",
    "first" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-01T03%3A10%3A07.160Z&to=2016-07-08T03%3A53%3A52.418Z&qp=CommonName.mdaas%7Eqqqqq&length=10&offset=0",
    "self" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-01T03%3A10%3A07.160Z&to=2016-07-08T03%3A53%3A52.418Z&qp=CommonName.mdaas%7Eqqqqq&length=10&offset=0",
    "next" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-01T03%3A10%3A07.160Z&to=2016-07-08T03%3A53%3A52.418Z&qp=CommonName.mdaas%7Eqqqqq&length=10&offset=10",
    "last" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-01T03%3A10%3A07.160Z&to=2016-07-08T03%3A53%3A52.418Z&qp=CommonName.mdaas%7Eqqqqq&length=10&offset=350"
      },
      "results" : {
        "type" : "SearchResults",
    "fromDate" : "2016-07-01T03:10:07.160Z",
    "toDate" : "2016-07-08T03:53:52.418Z",
    "total" : 354,
    "offset" : 0,
    "length" : 10,
    "infotons" : [ {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "c780fe7c4a835a36ce386bbd81be0240",
        "lastModified" : "2016-07-08T03:53:52.418Z",
        "path" : "/permid.org/1-21591414360",
        "dataCenter" : "dc1",
        "indexTime" : 1467950034046,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "QQQD1W2HN6:OX" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IlxID.mdaas" : [ "2QQQ1DH/N6-OC,USD,NORM" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "CommonName.mdaas" : [ "QQQPo ND WK JUL6" ],
        "RCSAssetClass.mdaas" : [ "FUT" ],
        "TRCSAssetClass.mdaas" : [ "Equity Futures" ],
        "ExchangeTicker.mdaas" : [ "QQQ2H" ],
        "QuoteExchangeCode.mdaas" : [ "ONE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "5fdbe8ee644edceaf364a296659482be",
        "lastModified" : "2016-07-08T00:24:10.707Z",
        "path" : "/permid.org/1-21591535341",
        "dataCenter" : "dc1",
        "indexTime" : 1467937451672,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "QQQD1W3FN6:OX" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "CommonName.mdaas" : [ "QQQPo ND WK JUL6" ],
        "RCSAssetClass.mdaas" : [ "FUT" ],
        "TRCSAssetClass.mdaas" : [ "Equity Futures" ],
        "ExchangeTicker.mdaas" : [ "QQQ3F" ],
        "QuoteExchangeCode.mdaas" : [ "ONE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "d378e665e282dc62bcdff76747c64a5a",
        "lastModified" : "2016-07-07T04:14:42.847Z",
        "path" : "/permid.org/1-21591343981",
        "dataCenter" : "dc1",
        "indexTime" : 1467864883985,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "QQQD1W2WN6:OX" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IlxID.mdaas" : [ "2QQQ1DW/N6-OC,USD,NORM" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "CommonName.mdaas" : [ "QQQPo ND WK JUL6" ],
        "RCSAssetClass.mdaas" : [ "FUT" ],
        "TRCSAssetClass.mdaas" : [ "Equity Futures" ],
        "ExchangeTicker.mdaas" : [ "QQQ2W" ],
        "QuoteExchangeCode.mdaas" : [ "ONE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "49ebf361fcae4d04af02df231e383deb",
        "lastModified" : "2016-07-06T03:19:41.203Z",
        "path" : "/permid.org/1-21591285520",
        "dataCenter" : "dc1",
        "indexTime" : 1467775238523,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "QQQD1W2MN6:OX" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IlxID.mdaas" : [ "2QQQ1DM/N6-OC,USD,NORM" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "CommonName.mdaas" : [ "QQQPo ND WK JUL6" ],
        "RCSAssetClass.mdaas" : [ "FUT" ],
        "TRCSAssetClass.mdaas" : [ "Equity Futures" ],
        "ExchangeTicker.mdaas" : [ "QQQ2M" ],
        "QuoteExchangeCode.mdaas" : [ "ONE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "edb0aef87075bad3d8c6e8767e1cbbf9",
        "lastModified" : "2016-07-06T03:19:40.964Z",
        "path" : "/permid.org/1-21591285350",
        "dataCenter" : "dc1",
        "indexTime" : 1467775187102,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "QQQD1W2TN6:OX" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IlxID.mdaas" : [ "2QQQ1DT/N6-OC,USD,NORM" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "CommonName.mdaas" : [ "QQQPo ND WK JUL6" ],
        "RCSAssetClass.mdaas" : [ "FUT" ],
        "TRCSAssetClass.mdaas" : [ "Equity Futures" ],
        "ExchangeTicker.mdaas" : [ "QQQ2T" ],
        "QuoteExchangeCode.mdaas" : [ "ONE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "3d00c221fa4cc4036efa00bcfec33f59",
        "lastModified" : "2016-07-05T02:43:57.872Z",
        "path" : "/permid.org/1-21590488985",
        "dataCenter" : "dc1",
        "indexTime" : 1467686639607,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "QQQD1W1FN6:OX^1" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IlxID.mdaas" : [ "1QQQ1DF/N6-OC,USD,NORM" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "CommonName.mdaas" : [ "QQQPo ND WK JUL6" ],
        "RCSAssetClass.mdaas" : [ "FUT" ],
        "TRCSAssetClass.mdaas" : [ "Equity Futures" ],
        "ExchangeTicker.mdaas" : [ "QQQ1F" ],
        "QuoteExchangeCode.mdaas" : [ "ONE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "19547d71542eaa8cd3c55c38e8065a5e",
        "lastModified" : "2016-07-04T02:23:47.608Z",
        "path" : "/permid.org/1-21590381247",
        "dataCenter" : "dc1",
        "indexTime" : 1467599028822,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "QQQD1W5HM6:OX^1" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IlxID.mdaas" : [ "5QQQ1DH/M6-OC,USD,NORM" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "CommonName.mdaas" : [ "QQQPo ND WK JUN6" ],
        "RCSAssetClass.mdaas" : [ "FUT" ],
        "TRCSAssetClass.mdaas" : [ "Equity Futures" ],
        "ExchangeTicker.mdaas" : [ "QQQ5H" ],
        "QuoteExchangeCode.mdaas" : [ "ONE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "6e3449bc916a5b9112e2971b2848d696",
        "lastModified" : "2016-07-03T06:45:38.168Z",
        "path" : "/permid.org/1-21590310413",
        "dataCenter" : "dc1",
        "indexTime" : 1467528339678,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "QQQD1W5WM6:OX^1" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IlxID.mdaas" : [ "5QQQ1DW/M6-OC,USD,NORM" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "CommonName.mdaas" : [ "QQQPo ND WK JUN6" ],
        "RCSAssetClass.mdaas" : [ "FUT" ],
        "TRCSAssetClass.mdaas" : [ "Equity Futures" ],
        "ExchangeTicker.mdaas" : [ "QQQ5W" ],
        "QuoteExchangeCode.mdaas" : [ "ONE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "eccf30dbdf6d72fd74fd3fc94bc62434",
        "lastModified" : "2016-07-02T01:28:52.291Z",
        "path" : "/permid.org/1-21590268843",
        "dataCenter" : "dc1",
        "indexTime" : 1467422934086,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "QQQD1W4TM6:OX^1" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IlxID.mdaas" : [ "4QQQ1DT/M6-OC,USD,NORM" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "CommonName.mdaas" : [ "QQQPo ND WK JUN6" ],
        "RCSAssetClass.mdaas" : [ "FUT" ],
        "TRCSAssetClass.mdaas" : [ "Equity Futures" ],
        "ExchangeTicker.mdaas" : [ "QQQ4T" ],
        "QuoteExchangeCode.mdaas" : [ "ONE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "b1902ece02d0184c39bac93fc36cbff0",
        "lastModified" : "2016-07-01T03:10:07.160Z",
        "path" : "/permid.org/1-21590006231",
        "dataCenter" : "dc1",
        "indexTime" : 1467342607863,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "QQQD1W4MM6:OX^1" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IlxID.mdaas" : [ "4QQQ1DM/M6-OC,USD,NORM" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "CommonName.mdaas" : [ "QQQPo ND WK JUN6" ],
        "RCSAssetClass.mdaas" : [ "FUT" ],
        "TRCSAssetClass.mdaas" : [ "Equity Futures" ],
        "ExchangeTicker.mdaas" : [ "QQQ4M" ],
        "QuoteExchangeCode.mdaas" : [ "ONE" ]
      }
    } ]
      }
    }
        
### Numeric Comparison ###

**Action:** Get up to 3 entities under permid.org, whose dsQuotationNumber value is greater than 950000. The field condition uses the `>` greater-than operator.

**Numeric comparison Curl command:**

    curl "<cm-well-host>/permid.org?op=search&qp=dsQuotationNumber.mdaas>950000&format=json&pretty&with-data&length=3"

**Numeric comparison response:**

    {
      "type" : "SearchResponse",
      "pagination" : {
    "type" : "PaginationInfo",
    "first" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T09%3A39%3A09.320Z&to=2016-07-08T11%3A54%3A49.121Z&qp=dsQuotationNumber.mdaas%3E950000&length=3&offset=0",
    "self" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T09%3A39%3A09.320Z&to=2016-07-08T11%3A54%3A49.121Z&qp=dsQuotationNumber.mdaas%3E950000&length=3&offset=0",
    "next" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T09%3A39%3A09.320Z&to=2016-07-08T11%3A54%3A49.121Z&qp=dsQuotationNumber.mdaas%3E950000&length=3&offset=3",
    "last" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T09%3A39%3A09.320Z&to=2016-07-08T11%3A54%3A49.121Z&qp=dsQuotationNumber.mdaas%3E950000&length=3&offset=17736"
      },
      "results" : {
    "type" : "SearchResults",
    "fromDate" : "2016-07-08T09:39:09.320Z",
    "toDate" : "2016-07-08T11:54:49.121Z",
    "total" : 17737,
    "offset" : 0,
    "length" : 3,
    "infotons" : [ {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "2afef8830546e1330ce2d6e8bd9b9527",
        "lastModified" : "2016-07-08T11:54:49.121Z",
        "path" : "/permid.org/1-21509276862",
        "dataCenter" : "dc1",
        "indexTime" : 1467978889973,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "IE0082945.I" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IsQuoteOf.mdaas" : [ "http://permid.org/1-8590050227" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "MIC.mdaas" : [ "XDUB" ],
        "CommonName.mdaas" : [ "BARING AUSTRALIA A OF" ],
        "RCSAssetClass.mdaas" : [ "OPF" ],
        "TRCSAssetClass.mdaas" : [ "Open-End Funds" ],
        "dsQuotationNumber.mdaas" : [ "966075" ],
        "QuoteExchangeCode.mdaas" : [ "ISE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "4ae7630a8100b67984a56c848c26e49b",
        "lastModified" : "2016-07-08T11:54:49.121Z",
        "path" : "/permid.org/1-21509276861",
        "dataCenter" : "dc1",
        "indexTime" : 1467978889974,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "IE0082923.I" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IsQuoteOf.mdaas" : [ "http://permid.org/1-8590293290" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "MIC.mdaas" : [ "XDUB" ],
        "CommonName.mdaas" : [ "BARING HONG KONG IT A OF" ],
        "RCSAssetClass.mdaas" : [ "OPF" ],
        "TRCSAssetClass.mdaas" : [ "Open-End Funds" ],
        "dsQuotationNumber.mdaas" : [ "966489" ],
        "QuoteExchangeCode.mdaas" : [ "ISE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "eff4fe6512ade35469157be12250c19e",
        "lastModified" : "2016-07-08T09:39:09.320Z",
        "path" : "/permid.org/1-21567567815",
        "dataCenter" : "dc1",
        "indexTime" : 1467970750181,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "2H6.F^G16" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IsQuoteOf.mdaas" : [ "http://permid.org/1-21566148656" ],
        "IlxID.mdaas" : [ "2H6-FF,EUR,NORM" ],
        "dsQuotationMnemonic.mdaas" : [ "D:2H6" ],
        "IsTradingIn.mdaas" : [ "EUR" ],
        "MIC.mdaas" : [ "XFRA" ],
        "CommonName.mdaas" : [ "HORIZON GLOBAL ORD" ],
        "RCSAssetClass.mdaas" : [ "ORD" ],
        "TRCSAssetClass.mdaas" : [ "Ordinary Shares" ],
        "dsQuotationNumber.mdaas" : [ "9818F6" ],
        "ExchangeTicker.mdaas" : [ "2H6" ],
        "QuoteExchangeCode.mdaas" : [ "FRA" ]
      }
    } ]
      }
    }
        
<a name="hdr4"></a>
## 4. Specify Output Fields ##

**Action:** Get only the CommonName and RCSAssetClass fields for up to 3 entities under permid.org, whose CommonName value contains the string "Reuters".

**Curl command:**

    curl "<cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Reuters&format=json&pretty&with-data=n3&fields=CommonName.mdaas,RCSAssetClass.mdaas&length=3"

**Response:**

    {
      "type" : "SearchResponse",
      "pagination" : {
        "type" : "PaginationInfo",
    "first" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A40%3A01.740Z&to=2016-07-08T15%3A40%3A01.979Z&qp=CommonName.mdaas%3AReuters&length=3&offset=0",
    "self" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A40%3A01.740Z&to=2016-07-08T15%3A40%3A01.979Z&qp=CommonName.mdaas%3AReuters&length=3&offset=0",
    "next" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A40%3A01.740Z&to=2016-07-08T15%3A40%3A01.979Z&qp=CommonName.mdaas%3AReuters&length=3&offset=3",
    "last" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A40%3A01.740Z&to=2016-07-08T15%3A40%3A01.979Z&qp=CommonName.mdaas%3AReuters&length=3&offset=70914"
      },
      "results" : {
        "type" : "SearchResults",
    "fromDate" : "2016-07-08T15:40:01.740Z",
    "toDate" : "2016-07-08T15:40:01.979Z",
    "total" : 70914,
    "offset" : 0,
    "length" : 3,
    "infotons" : [ {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "08e369b8d4660826eb9494810e0af09b",
        "lastModified" : "2016-07-08T15:40:01.979Z",
        "path" : "/permid.org/1-21591377740",
        "dataCenter" : "dc1",
        "indexTime" : 1467992402553,
        "parent" : "/permid.org"
      },
      "fields" : {
        "CommonName.mdaas" : [ "Goldman Sachs & Co Wertpapier GMBH Call 1313.7255 USD Thomson Reuters: Gold / US Dollar FX Spot Rate 31Dec99" ],
        "RCSAssetClass.mdaas" : [ "BARWNT" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "c8fc5e38b51892b529caa66646d6a56a",
        "lastModified" : "2016-07-08T15:40:01.940Z",
        "path" : "/permid.org/1-21591377619",
        "dataCenter" : "dc1",
        "indexTime" : 1467992403912,
        "parent" : "/permid.org"
      },
      "fields" : {
        "CommonName.mdaas" : [ "Goldman Sachs & Co Wertpapier GMBH Call 1240 USD Thomson Reuters: Gold / US Dollar FX Spot Rate 31Dec99" ],
        "RCSAssetClass.mdaas" : [ "BARWNT" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "f188963093cd7b90a169c81bb8973daf",
        "lastModified" : "2016-07-08T15:40:01.740Z",
        "path" : "/permid.org/1-21591374238",
        "dataCenter" : "dc1",
        "indexTime" : 1467992402499,
        "parent" : "/permid.org"
      },
      "fields" : {
        "CommonName.mdaas" : [ "COMMERZBANK AG Put 0.0001 USD Thomson Reuters: Gold / US Dollar FX Spot Rate 13Dec16" ],
        "RCSAssetClass.mdaas" : [ "BARWNT" ]
      }
    } ]
      }
    }

<a name="hdr5"></a>
## 5. Implement AND, OR, NOT ##

### Multiple Conditions ("AND")  ###

**Action:** Get up to 3 entities under permid.org, whose CommonName value contains the string "Reuters", and whose RCSAssetClass value contains the string "TRAD".

**Curl command:**

    curl "<cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Reuters,RCSAssetClass.mdaas:TRAD&format=json&pretty&length=3&with-data"

**Response:**

    {
      "type" : "SearchResponse",
      "pagination" : {
    "type" : "PaginationInfo",
    "first" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A25%3A07.856Z&to=2016-07-08T15%3A25%3A07.856Z&qp=CommonName.mdaas%3AReuters%2CRCSAssetClass.mdaas%3ATRAD&length=3&offset=0",
    "self" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A25%3A07.856Z&to=2016-07-08T15%3A25%3A07.856Z&qp=CommonName.mdaas%3AReuters%2CRCSAssetClass.mdaas%3ATRAD&length=3&offset=0",
    "next" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A25%3A07.856Z&to=2016-07-08T15%3A25%3A07.856Z&qp=CommonName.mdaas%3AReuters%2CRCSAssetClass.mdaas%3ATRAD&length=3&offset=3",
    "last" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T15%3A25%3A07.856Z&to=2016-07-08T15%3A25%3A07.856Z&qp=CommonName.mdaas%3AReuters%2CRCSAssetClass.mdaas%3ATRAD&length=3&offset=8592"
      },
      "results" : {
    "type" : "SearchResults",
    "fromDate" : "2016-07-08T15:25:07.856Z",
    "toDate" : "2016-07-08T15:25:07.856Z",
    "total" : 8593,
    "offset" : 0,
    "length" : 3,
    "infotons" : [ {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "83d204f7495ebef595724af60bdcd83d",
        "lastModified" : "2016-07-08T15:25:07.856Z",
        "path" : "/permid.org/1-21591362718",
        "dataCenter" : "dc1",
        "indexTime" : 1467991508847,
        "parent" : "/permid.org"
      },
      "fields" : {
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Instrument" ],
        "instrumentExpiryDate.mdaas" : [ "2017-06-16" ],
        "isIssuedBy.mdaas" : [ "http://permid.org/1-5000008825" ],
        "CommonName.mdaas" : [ "Vontobel Financial Products Call 31 USD Thomson Reuters: Silver / US Dollar FX Spot Rate 16Jun17" ],
        "instrumentCurrencyName.mdaas" : [ "EUR" ],
        "instrumentStatus.mdaas" : [ "Active" ],
        "RCSAssetClass.mdaas" : [ "TRAD" ],
        "instrumentAssetClass.mdaas" : [ "Traditional Warrants" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "c2c05f8e258c6b33fcb6e53c84e93d2c",
        "lastModified" : "2016-07-08T15:25:07.856Z",
        "path" : "/permid.org/1-21591362720",
        "dataCenter" : "dc1",
        "indexTime" : 1467991508849,
        "parent" : "/permid.org"
      },
      "fields" : {
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Instrument" ],
        "instrumentExpiryDate.mdaas" : [ "2016-08-19" ],
        "isIssuedBy.mdaas" : [ "http://permid.org/1-5000008825" ],
        "CommonName.mdaas" : [ "Vontobel Financial Products Call 24.5 USD Thomson Reuters: Silver / US Dollar FX Spot Rate 19Aug16" ],
        "instrumentCurrencyName.mdaas" : [ "EUR" ],
        "instrumentStatus.mdaas" : [ "Active" ],
        "RCSAssetClass.mdaas" : [ "TRAD" ],
        "instrumentAssetClass.mdaas" : [ "Traditional Warrants" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "84e8ea6d184fb1b68d88eba85e6ba031",
        "lastModified" : "2016-07-08T15:25:07.856Z",
        "path" : "/permid.org/1-21591362722",
        "dataCenter" : "dc1",
        "indexTime" : 1467991508846,
        "parent" : "/permid.org"
      },
      "fields" : {
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Instrument" ],
        "instrumentExpiryDate.mdaas" : [ "2016-09-16" ],
        "isIssuedBy.mdaas" : [ "http://permid.org/1-5000008825" ],
        "CommonName.mdaas" : [ "Vontobel Financial Products Call 26 USD Thomson Reuters: Silver / US Dollar FX Spot Rate 16Sep16" ],
        "instrumentCurrencyName.mdaas" : [ "EUR" ],
        "instrumentStatus.mdaas" : [ "Active" ],
        "RCSAssetClass.mdaas" : [ "TRAD" ],
        "instrumentAssetClass.mdaas" : [ "Traditional Warrants" ]
      }
    } ]
      }
    }
    
### Optional Conditions ("OR") ###

**Action:** Get up to 5 entities under permid.org, whose organizationStateProvince value contains either "Delaware" or "Idaho".

**Curl command:**

    curl "<cm-well-host>/permid.org?op=search&qp=*organizationStateProvince.mdaas:Delaware,*organizationStateProvince.mdaas:Idaho&format=json&pretty&with-data&length=5&fields=organizationStateProvince.mdaas,organizationCity.mdaas"

**Response:**

    {
      "type" : "SearchResponse",
      "pagination" : {
    "type" : "PaginationInfo",
    "first" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T12%3A55%3A25.659Z&to=2016-07-08T16%3A10%3A15.331Z&qp=*organizationStateProvince.mdaas%3ADelaware%2C*organizationStateProvince.mdaas%3AIdaho&length=5&offset=0",
    "self" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T12%3A55%3A25.659Z&to=2016-07-08T16%3A10%3A15.331Z&qp=*organizationStateProvince.mdaas%3ADelaware%2C*organizationStateProvince.mdaas%3AIdaho&length=5&offset=0",
    "next" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T12%3A55%3A25.659Z&to=2016-07-08T16%3A10%3A15.331Z&qp=*organizationStateProvince.mdaas%3ADelaware%2C*organizationStateProvince.mdaas%3AIdaho&length=5&offset=5",
    "last" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T12%3A55%3A25.659Z&to=2016-07-08T16%3A10%3A15.331Z&qp=*organizationStateProvince.mdaas%3ADelaware%2C*organizationStateProvince.mdaas%3AIdaho&length=5&offset=129660"
      },
      "results" : {
    "type" : "SearchResults",
    "fromDate" : "2016-07-08T12:55:25.659Z",
    "toDate" : "2016-07-08T16:10:15.331Z",
    "total" : 129664,
    "offset" : 0,
    "length" : 5,
    "infotons" : [ {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "ede36a0643a6273085cc782d861a811a",
        "lastModified" : "2016-07-08T16:10:15.331Z",
        "path" : "/permid.org/1-5046728258",
        "dataCenter" : "dc1",
        "indexTime" : 1467994216087,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "HANOVER" ],
        "organizationStateProvince.mdaas" : [ "DELAWARE", "NEW HAMPSHIRE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "5518ea6ac23692ce30caa24b9c868ea6",
        "lastModified" : "2016-07-08T15:25:13.079Z",
        "path" : "/permid.org/1-5049099159",
        "dataCenter" : "dc1",
        "indexTime" : 1467991566404,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "SAN FRANCISCO" ],
        "organizationStateProvince.mdaas" : [ "CALIFORNIA", "DELAWARE" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "65582f41fd62f409d3f6c4990c7af844",
        "lastModified" : "2016-07-08T15:25:12.687Z",
        "path" : "/permid.org/1-5001360647",
        "dataCenter" : "dc1",
        "indexTime" : 1467991566572,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "MOUNTAIN HOME" ],
        "organizationStateProvince.mdaas" : [ "IDAHO" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "eaa7fe2bff508f77e203816c03f2d952",
        "lastModified" : "2016-07-08T15:25:12.427Z",
        "path" : "/permid.org/1-4297939554",
        "dataCenter" : "dc1",
        "indexTime" : 1467991513916,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "IDAHO FALLS" ],
        "organizationStateProvince.mdaas" : [ "IDAHO" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "cc60f9c1ccf71019861f7277abcfd3ed",
        "lastModified" : "2016-07-08T12:55:25.659Z",
        "path" : "/permid.org/1-5051394129",
        "dataCenter" : "dc1",
        "indexTime" : 1467982577997,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "EMMETT" ],
        "organizationStateProvince.mdaas" : [ "IDAHO" ]
      }
    } ]
      }
    }  

### Negative Conditions ("NOT") ###

**Action:** Get up to 5 entities under permid.org, whose organizationStateProvince value contains "New York", but whose organizationCity value does not contain "New York" (i.e. the entity is located in New York state but not in New York city).


**Curl command:**

    curl "<cm-well-host>/permid.org?op=search&qp=organizationStateProvince.mdaas:New%20York,-organizationCity.mdaas:new%20york&format=json&pretty&with-data&length=5&fields=organizationStateProvince.mdaas,organizationCity.mdaas"

**Response:**

    {
      "type" : "SearchResponse",
      "pagination" : {
    "type" : "PaginationInfo",
    "first" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T10%3A39%3A27.444Z&to=2016-07-08T15%3A25%3A13.470Z&qp=organizationStateProvince.mdaas%3ANew+York%2C-organizationCity.mdaas%3Anew+york&length=5&offset=0",
    "self" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T10%3A39%3A27.444Z&to=2016-07-08T15%3A25%3A13.470Z&qp=organizationStateProvince.mdaas%3ANew+York%2C-organizationCity.mdaas%3Anew+york&length=5&offset=0",
    "next" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T10%3A39%3A27.444Z&to=2016-07-08T15%3A25%3A13.470Z&qp=organizationStateProvince.mdaas%3ANew+York%2C-organizationCity.mdaas%3Anew+york&length=5&offset=5",
    "last" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T10%3A39%3A27.444Z&to=2016-07-08T15%3A25%3A13.470Z&qp=organizationStateProvince.mdaas%3ANew+York%2C-organizationCity.mdaas%3Anew+york&length=5&offset=68905"
      },
      "results" : {
    "type" : "SearchResults",
    "fromDate" : "2016-07-08T10:39:27.444Z",
    "toDate" : "2016-07-08T15:25:13.470Z",
    "total" : 68905,
    "offset" : 0,
    "length" : 5,
    "infotons" : [ {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "b54c657343f7146989a6cda1fcdc6f64",
        "lastModified" : "2016-07-08T15:25:13.470Z",
        "path" : "/permid.org/1-5051394161",
        "dataCenter" : "dc1",
        "indexTime" : 1467991566029,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "GLENDALE" ],
        "organizationStateProvince.mdaas" : [ "NEW YORK" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "cbed49332e66edd289da9481f8f3ac83",
        "lastModified" : "2016-07-08T14:10:04.847Z",
        "path" : "/permid.org/1-5051394157",
        "dataCenter" : "dc1",
        "indexTime" : 1467987058263,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "BROOKLYN" ],
        "organizationStateProvince.mdaas" : [ "NEW YORK" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "6d9600d422e7130b4309205af7f0f456",
        "lastModified" : "2016-07-08T14:10:04.319Z",
        "path" : "/permid.org/1-5034764167",
        "dataCenter" : "dc1",
        "indexTime" : 1467987005379,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "BROOKLYN" ],
        "organizationStateProvince.mdaas" : [ "NEW YORK" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "5c7476f29384a2f680e87ec95a8c2b9f",
        "lastModified" : "2016-07-08T12:55:25.206Z",
        "path" : "/permid.org/1-5006279969",
        "dataCenter" : "dc1",
        "indexTime" : 1467982578452,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "WHITE PLAINS" ],
        "organizationStateProvince.mdaas" : [ "NEW YORK" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "740bb1c6ea1f85e169f96edb2cdd3567",
        "lastModified" : "2016-07-08T10:39:27.444Z",
        "path" : "/permid.org/1-5051393857",
        "dataCenter" : "dc1",
        "indexTime" : 1467974417952,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "RIDGEWOOD" ],
        "organizationStateProvince.mdaas" : [ "NEW YORK" ]
      }
    } ]
      }
    }  
    
<a name="hdr6"></a>
## 6. Page through Results ##

**Action:** Get the first 3 entities (offset=0, length=3) under permid.org, whose organizationStateProvince value contains "New York" .

**Curl command:**

    curl "<cm-well-host>/permid.org?op=search&qp=organizationStateProvince.mdaas:New%20York&format=json&pretty&with-data&offset=0&length=3&fields=organizationStateProvince.mdaas,organizationCity.mdaas"

**Response:**

    {
      "type" : "SearchResponse",
      "pagination" : {
    "type" : "PaginationInfo",
    "first" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T14%3A10%3A04.439Z&to=2016-07-08T15%3A25%3A13.470Z&qp=organizationStateProvince.mdaas%3ANew+York&length=3&offset=0",
    "self" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T14%3A10%3A04.439Z&to=2016-07-08T15%3A25%3A13.470Z&qp=organizationStateProvince.mdaas%3ANew+York&length=3&offset=0",
    "next" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T14%3A10%3A04.439Z&to=2016-07-08T15%3A25%3A13.470Z&qp=organizationStateProvince.mdaas%3ANew+York&length=3&offset=3",
    "last" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T14%3A10%3A04.439Z&to=2016-07-08T15%3A25%3A13.470Z&qp=organizationStateProvince.mdaas%3ANew+York&length=3&offset=123936"
      },
      "results" : {
    "type" : "SearchResults",
    "fromDate" : "2016-07-08T14:10:04.439Z",
    "toDate" : "2016-07-08T15:25:13.470Z",
    "total" : 123938,
    "offset" : 0,
    "length" : 3,
    "infotons" : [ {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "b54c657343f7146989a6cda1fcdc6f64",
        "lastModified" : "2016-07-08T15:25:13.470Z",
        "path" : "/permid.org/1-5051394161",
        "dataCenter" : "dc1",
        "indexTime" : 1467991566029,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "GLENDALE" ],
        "organizationStateProvince.mdaas" : [ "NEW YORK" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "cbed49332e66edd289da9481f8f3ac83",
        "lastModified" : "2016-07-08T14:10:04.847Z",
        "path" : "/permid.org/1-5051394157",
        "dataCenter" : "dc1",
        "indexTime" : 1467987058263,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "BROOKLYN" ],
        "organizationStateProvince.mdaas" : [ "NEW YORK" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "4055b393538b016e7a05457301ea669b",
        "lastModified" : "2016-07-08T14:10:04.439Z",
        "path" : "/permid.org/1-5038060478",
        "dataCenter" : "dc1",
        "indexTime" : 1467987006101,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "NEW YORK" ],
        "organizationStateProvince.mdaas" : [ "NEW YORK" ]
      }
    } ]
      }
    }   
    
**Action:** Get the next 3 entities (offset=3, length=3) under permid.org, whose organizationStateProvince value contains "New York".

**Curl command:**

    curl "<cm-well-host>/permid.org?op=search&qp=organizationStateProvince.mdaas:New%20York&format=json&pretty&with-data&offset=3&length=3&fields=organizationStateProvince.mdaas,organizationCity.mdaas"

**Response:**

    {
      "type" : "SearchResponse",
      "pagination" : {
    "type" : "PaginationInfo",
    "first" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T10%3A39%3A27.503Z&to=2016-07-08T14%3A10%3A04.319Z&qp=organizationStateProvince.mdaas%3ANew+York&length=3&offset=0",
    "previous" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T10%3A39%3A27.503Z&to=2016-07-08T14%3A10%3A04.319Z&qp=organizationStateProvince.mdaas%3ANew+York&length=3&offset=0",
    "self" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T10%3A39%3A27.503Z&to=2016-07-08T14%3A10%3A04.319Z&qp=organizationStateProvince.mdaas%3ANew+York&length=3&offset=3",
    "next" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T10%3A39%3A27.503Z&to=2016-07-08T14%3A10%3A04.319Z&qp=organizationStateProvince.mdaas%3ANew+York&length=3&offset=6",
    "last" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-08T10%3A39%3A27.503Z&to=2016-07-08T14%3A10%3A04.319Z&qp=organizationStateProvince.mdaas%3ANew+York&length=3&offset=123936"
      },
      "results" : {
    "type" : "SearchResults",
    "fromDate" : "2016-07-08T10:39:27.503Z",
    "toDate" : "2016-07-08T14:10:04.319Z",
    "total" : 123938,
    "offset" : 3,
    "length" : 3,
    "infotons" : [ {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "6d9600d422e7130b4309205af7f0f456",
        "lastModified" : "2016-07-08T14:10:04.319Z",
        "path" : "/permid.org/1-5034764167",
        "dataCenter" : "dc1",
        "indexTime" : 1467987005379,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "BROOKLYN" ],
        "organizationStateProvince.mdaas" : [ "NEW YORK" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "5c7476f29384a2f680e87ec95a8c2b9f",
        "lastModified" : "2016-07-08T12:55:25.206Z",
        "path" : "/permid.org/1-5006279969",
        "dataCenter" : "dc1",
        "indexTime" : 1467982578452,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "WHITE PLAINS" ],
        "organizationStateProvince.mdaas" : [ "NEW YORK" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "73cead0a965c98ea9e801fcf021284bc",
        "lastModified" : "2016-07-08T10:39:27.503Z",
        "path" : "/permid.org/1-5051394063",
        "dataCenter" : "dc1",
        "indexTime" : 1467974418394,
        "parent" : "/permid.org"
      },
      "fields" : {
        "organizationCity.mdaas" : [ "NEW YORK" ],
        "organizationStateProvince.mdaas" : [ "NEW YORK" ]
      }
    } ]
      }
    }
    
<a name="hdr7"></a>
## 7. Traverse Inbound and Outbound Links ##

**Action:** Find the address of the company that issues a quote with a RIC code of "VAC". 
The query uses the **yg** flag that enables traversing inbound and outbound links.

The steps in the query are:

1. Find quotes with a RIC code of "VAC".
2. Follow the quote's outbound links to find out what instrument issues the quote.
3. Follow the instrument's inbound links to find out which company points to the instrument in its primaryInstrument field.

**Curl command:**

    curl "<cm-well-host>/permid.org?op=search&qp=RIC.mdaas::VAC&with-data&yg=>IsQuoteOf.mdaas<primaryInstrument.mdaas&format=json&pretty"

**Response:**

    {
      "type" : "SearchResponse",
      "pagination" : {
    "type" : "PaginationInfo",
    "first" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2015-03-24T11%3A39%3A46.462Z&to=2015-03-24T11%3A39%3A46.462Z&qp=RIC.mdaas%3A%3AVAC&length=3&offset=0",
    "self" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2015-03-24T11%3A39%3A46.462Z&to=2015-03-24T11%3A39%3A46.462Z&qp=RIC.mdaas%3A%3AVAC&length=3&offset=0",
    "last" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2015-03-24T11%3A39%3A46.462Z&to=2015-03-24T11%3A39%3A46.462Z&qp=RIC.mdaas%3A%3AVAC&length=3&offset=0"
      },
      "results" : {
    "type" : "SearchResults",
    "fromDate" : "2015-03-24T11:39:46.462Z",
    "toDate" : "2015-03-24T11:39:46.462Z",
    "total" : 1,
    "offset" : 0,
    "length" : 3,
    "infotons" : [ {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "0dcfaba91276c59c448bad93cc7e232e",
        "lastModified" : "2015-03-24T11:39:46.462Z",
        "path" : "/permid.org/1-21478428449",
        "dataCenter" : "dc1",
        "indexTime" : 1460125129760,
        "parent" : "/permid.org"
      },
      "fields" : {
        "RIC.mdaas" : [ "VAC" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Quote" ],
        "IsQuoteOf.mdaas" : [ "http://permid.org/1-21478428282" ],
        "IlxID.mdaas" : [ "VAC-US,USD,NORM" ],
        "dsQuotationMnemonic.mdaas" : [ "U:VAC" ],
        "IsTradingIn.mdaas" : [ "USD" ],
        "MIC.mdaas" : [ "XXXX" ],
        "CommonName.mdaas" : [ "MARRIOTT VACATIONS WORLDWIDE ORD" ],
        "RCSAssetClass.mdaas" : [ "ORD" ],
        "TRCSAssetClass.mdaas" : [ "Ordinary Shares" ],
        "dsQuotationNumber.mdaas" : [ "77857E" ],
        "ExchangeTicker.mdaas" : [ "VAC" ],
        "QuoteExchangeCode.mdaas" : [ "NYQ" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "af45a50bfe5645b16ef1ccccd2ab3006",
        "lastModified" : "2015-03-08T12:38:04.543Z",
        "path" : "/permid.org/1-21478428282",
        "dataCenter" : "dc1",
        "indexTime" : 1460133028465,
        "parent" : "/permid.org"
      },
      "fields" : {
        "WorldscopeID.mdaas" : [ "57164Y107" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Instrument" ],
        "WorldScopePermId.mdaas" : [ "C840UV2P0" ],
        "mainQuoteId.mdaas" : [ "21478428456" ],
        "isIssuedBy.mdaas" : [ "http://permid.org/1-5035948006" ],
        "CommonName.mdaas" : [ "Marriott Vacations Worldwide Ord Shs" ],
        "instrumentCurrencyName.mdaas" : [ "USD" ],
        "instrumentStatus.mdaas" : [ "Active" ],
        "RCSAssetClass.mdaas" : [ "ORD" ],
        "instrumentAssetClass.mdaas" : [ "Ordinary Shares" ]
      }
    }, {
      "type" : "ObjectInfoton",
      "system" : {
        "uuid" : "901803c75dd49427a4cba92878095e4b",
        "lastModified" : "2016-04-06T07:15:07.612Z",
        "path" : "/permid.org/1-5035948006",
        "dataCenter" : "dc1",
        "indexTime" : 1460098385554,
        "parent" : "/permid.org"
      },
      "fields" : {
        "legalRegistrationCommonAddress.mdaas" : [ "2711 Centerville Rd\nWILMINGTON\nDELAWARE\n19808-1660\nUnited States\n" ],
        "primaryInstrument.mdaas" : [ "http://permid.org/1-21478428282" ],
        "type.rdf" : [ "http://ont.thomsonreuters.com/mdaas/Organization" ],
        "organizationAddressLine1.mdaas" : [ "6649 Westwood Blvd", "6649 Westwood Blvd", "2711 Centerville Rd" ],
        "organizationAddressLine2.mdaas" : [ "2711 Centerville Road" ],
        "MXID.mdaas" : [ "111800547" ],
        "PrimaryReportingEntityCode.mdaas" : [ "D9F64" ],
        "organizationCity.mdaas" : [ "ORLANDO", "WILMINGTON" ],
        "organizationFoundedDay.mdaas" : [ "21" ],
        "WorldscopeCompanyPermID.mdaas" : [ "C840UV2P0" ],
        "organizationCountryCode.mdaas" : [ "100319" ],
        "instrumentCount.mdaas" : [ "1" ],
        "organizationFoundedYear.mdaas" : [ "2011" ],
        "mainQuoteId.mdaas" : [ "21478428456" ],
        "CIK.mdaas" : [ "0001524358" ],
        "hasRegistrationAuthority.mdaas" : [ "http://permid.org/1-5000008957" ],
        "entityLastReviewedDate.mdaas" : [ "2016-03-02 05:00:00" ],
        "hasUltimateParent.mdaas" : [ "http://permid.org/1-5035948006" ],
        "OrganizationProviderTypeCode.mdaas" : [ "1" ],
        "organizationWebsite.mdaas" : [ "http://www.marriottvacationsworldwide.com/" ],
        "hasImmediateParent.mdaas" : [ "http://permid.org/1-5035948006" ],
        "LEI.mdaas" : [ "549300WA6BT5H4F7IO94" ],
        "CommonName.mdaas" : [ "Marriott Vacations Worldwide Corp" ],
        "headquartersCommonAddress.mdaas" : [ "6649 Westwood Blvd\nORLANDO\nFLORIDA\n32821-8029\nUnited States\n" ],
        "registeredFax.mdaas" : [ "13026365454" ],
        "organizationStateProvinceOfficialCode.mdaas" : [ "DE", "FL" ],
        "organizationStatusCode.mdaas" : [ "Active" ],
        "InvestextID.mdaas" : [ "VAC" ],
        "organizationFoundedMonth.mdaas" : [ "6" ],
        "officialName.mdaas" : [ "MARRIOTT VACATIONS WORLDWIDE CORPORATION" ],
        "headquartersPhone.mdaas" : [ "14072066000" ],
        "headquartersAddress.mdaas" : [ "ORLANDO\nFLORIDA\n32821-8029\nUnited States\n", "6649 Westwood Blvd\nORLANDO\nFLORIDA\n32821-8029\nUnited States\n" ],
        "isDomiciledIn.mdaas" : [ "United States" ],
        "organizationCountry.mdaas" : [ "United States" ],
        "SDCID.mdaas" : [ "1218547002          " ],
        "isPublicFlag.mdaas" : [ "true" ],
        "registeredPhone.mdaas" : [ "13026365400" ],
        "TaxID.mdaas" : [ "452598330" ],
        "organizationStateProvince.mdaas" : [ "DELAWARE", "FLORIDA" ],
        "legalRegistrationAddress.mdaas" : [ "WILMINGTON\nDELAWARE\n19808-1660\nUnited States\n", "2711 Centerville Rd\nWILMINGTON\nDELAWARE\n19808-1660\nUnited States\n" ],
        "subsidiariesCount.mdaas" : [ "109" ],
        "organizationTypeCode.mdaas" : [ "Business Organization" ],
        "organizationAddressPostalCode.mdaas" : [ "32821-8029", "19808-1660" ],
        "organizationSubtypeCode.mdaas" : [ "Company" ],
        "SDCusip.mdaas" : [ "56655E" ],
        "shortName.mdaas" : [ "Marriott Vaca" ],
        "equityInstrumentCount.mdaas" : [ "1" ],
        "organizationAddressLine3.mdaas" : [ "New Castle County" ],
        "isIncorporatedIn.mdaas" : [ "United States" ],
        "WorldscopeCompanyID.mdaas" : [ "57164Y107" ],
        "primaryIndustry.mdaas" : [ "Hotels, Motels & Cruise Lines (NEC)" ],
        "RCPID.mdaas" : [ "600304172" ]
      }
    } ]
      }
    }

## API Reference ##

[Query for Infotons Using Field Conditions](API.Query.QueryForInfotonsUsingFieldConditions.md)
[CM-Well Query Parameters](API.QueryParameters.md)
[Field Condition Syntax](API.FieldConditionSyntax.md)
