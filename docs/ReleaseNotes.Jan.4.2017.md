# CM-Well Version Release Notes - Jan. 4 2017 #

## Change Summary ##
The changes in this release include bug fixes and infrastructure improvements only - see next section.

### Notable Bug Fixes and Improvements ###

1. A bug was fixed that caused sorting on metadata fields to fail. Specifically, this caused **consume** actions and the data center synchronization that depended on them to skip some data, as they require a sort on the system field indexTime. **Note:** This bug may have also affected end user calls to the **consume** API, mostly when consuming historical time ranges, as opposed to the real-time "horizon".
2. Improvements and optimizations were made to the **bulk-consume** operation and its responsiveness to load.
2. Performance and stability improvements were made to data correction operations (**purge** and the administrator operation **x-fix**) as related to large infotons.
4. Ingestion of large infotons (>= 1MB) was improved. The previous handling could be very slow or fail occasionally.
5. Performance and stability improvements were made to the CM-Well storage layer so that load is leveled more evenly for the data layer.
6. Data center synchronization was improved such that it now does not pause or stall when it encounters invalid data. In this case, it writes the relevant info to the red error log and continues.
7. **Operations-related:** Per request from the Operations team, the **install** command has been renamed to the unambiguous **wipeandinstall**, so that it's clear that this command wipes all CM-Well data. An additional confirmation step was also added.

### Changes to API	 ###

None.


