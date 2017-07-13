# CM-Well Version Release Notes - Jan. 17 2017 #

## Change Summary ##
The changes in this release include bug fixes and infrastructure improvements only - see next section.

### Notable Bug Fixes and Improvements ###

1. Data center synchronization improvement. The data transfer that is managed during sync now switches automatically among 3 different modes: consume, bulk-consume and slow-bulk, depending on environment conditions. 
1. Improved resilience of the background process that writes data in the new format. Tasks are now transferred seamlessly among machines if a machine goes offline or comes back online.
1. Bug fix #285: Some whole-graph SPARQL queries timed out due to errors in the optimization logic. Here is an example of a query that timed out:

    ```
    SELECT ?x ?name
    WHERE {
     ?typebridge metadata:geographyType data:1-308005 .
     ?x metadata:geographyType ?typebridge .
     ?x metadata:geographyUniqueName ?name .
    }
    ```

1. Bug fix #292: Due to an error in caching, CM-Well would sometimes return the "pending" response when under stress.

### Changes to API	 ###

None.


