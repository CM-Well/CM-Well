# Managing Retries under Busy Server Conditions #

Under conditions of heavy load, you may receive the 503 (Service Unavailable) HTTP error from CM-Well. In this case, we recommend implementing the following client logic:

1. Sleep for 1 second, then retry the request.
2. If you still get the 503 error, sleep for a longer period (for instance, 2-3 seconds), then retry the request.
3. If you still experience the problem, retry periodically, while sleeping for longer periods each time, up to a maximum of 60 seconds.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.BestPractices.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](DevGuide.BestPractices.ConditionalUpdates.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](DevGuide.BestPractices.StreamingMethods.md)  

----