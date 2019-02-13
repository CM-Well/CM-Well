# From/To Datetime Formatting #

Datetime values of the optional **from** and **to** query parameters must be formatted according to the ISO 8601 standard, as follows:

`YYYY-MM-DDThh:mm:ss.sTZD` 

Where:

* YYYY = four-digit year
*  MM   = two-digit month (01=January, etc.)
*  DD   = two-digit day of month (01 through 31)
*  hh   = two digits of hour (00 through 23) (am/pm NOT allowed)
*  mm   = two digits of minute (00 through 59)
*  ss   = two digits of second (00 through 59)
*  s= one or more digits representing a decimal fraction of a second
*  TZD  = time zone designator (Z to indicate UTC (Coordinated Universal Time) or +hh:mm or -hh:mm to indicate the offset in hour and minutes from UTC.)

**Example:**

`1997-07-16T19:20:30.45Z`

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.FieldNameFormats.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.PagingThroughResultsWithOffsetAndLengthParameters.md)  

----
