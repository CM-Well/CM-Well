# Atom Release (May 2019)

!!! warning
	- Due to significant data format changes, you must perform a full install of the Atom version. You cannot perform an upgrade process and retain your existing data. 
	- To copy over your data to a CM-Well cluster with the new version, either re-ingest your data (the preferred option), or perform a DC-Sync process between the "old cluster" and the "new cluster" (see DC-Sync details below). 
	- After installing Atom, you will be able to perform an upgrade process for subsequent releases.

Title | Git Issue | Description 
:------|:----------|:------------
Major version upgrades to 3rd-party libraries | [1003](https://github.com/thomsonreuters/CM-Well/pull/1003), [1109](https://github.com/thomsonreuters/CM-Well/pull/1109) | Several 3rd-party libraries have been upgraded, including: Elasticsearch -> 7, Cassandra -> 3.11.4, Kafka -> 2.1.1. See note above about the need to perform a full install.
Use of AdoptOpenJDK | [1003](https://github.com/thomsonreuters/CM-Well/pull/1003) | The AdoptOpenJDK package is now used instead of Oracle's OpenJDK package.
New documentation site | [1045](https://github.com/thomsonreuters/CM-Well/pull/1045) | CM-Well now has a new documentation site (generated with the aid of the MkDocs tool). The new site has a user-friendly UI, including topic tabs, navigation menu, and full-text search. Check it out at [https://cm-well.github.io/CM-Well](https://cm-well.github.io/CM-Well).
Single Cassandra instance per node | [1082](https://github.com/thomsonreuters/CM-Well/pull/1082) | See details below.


### Changes to API

None.

### Migrating Data to Atom Format by DC-Sync

To perform a DC-Sync process as a means of migrating your data to the new Atom release, you will need a temporary cluster with no data on it, in addition to your operational cluster. The process would be as follows:

1. Perform DC-Sync from the operational cluster to the temporary cluster.
2. Perform a wipe-and-install of the Atom release on the operational cluster.
3. Perform DC-Sync from the temporary cluster to the operational cluster.

### Changes to Cassandra Management Architecture

Previous versions of Cassandra could only work with one disk per instance.
To accommodate Cassandra, CM-Well managed a "sub-division" architecture, whereby it ran 4 instances of Cassandra per node, each with its own JVM, disk and IP address. The unique IP address was required because the communication port could not be changed, therefore all instances used the same port. This also required manual alias configuration.

Due to changes in the latest Cassandra version, a single Cassandra instance can now manage several disks, making the complications described above unnecessary. Now there is a single Cassandra instance per node, managing several disks.






