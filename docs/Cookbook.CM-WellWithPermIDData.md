# Populating Your CM-Well Instance with PermID.org Data #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md)

----

## What is PermID.org? ##

You may or may not be familiar with the [PermID platform](https://permid.org/). It provides free, open access to Refinitiv financial entities and their Permanent Identifiers, via an easy-to-use REST API. If you haven't already, now's the time to check it out.

These are the entities that PermID supports:

* Organizations
* Instruments
* Quotes
* People
* Currencies
* Asset Classes
* Industry Codes

Consuming PermID.org's data is a free and painless way to populate your CM-Well platform with world-class financial linked data. This article shows you how to do just that.

## How do I Consume PermID.org Data? ##
The idea is simple. Create your initial repository by downloading a bulk file containing all the PermID entities of a certain type. Then all you have to do is periodically sample the PermID Atom Feed, which provides you with incremental updates and keeps your data up-to-the-minute.

It's important to note that each ingest to CM-Well is considered an "insert-or-update" operation, so if it's a new entity it's created, and if it's an existing entity it's updated. You don't have to guess which one is it, simply ingest all the data that the feed provides you.

The following steps show you how to download a bulk file from PermID.org, ingest it to a freshly installed CM-Well instance, and then create a task that keeps it current with incremental updates.

### Step 1: Create an Up-and-Running CM-Well Instance ###

See the [CM-Well GitHub Readme file](https://github.com/CM-Well/CM-Well/blob/master/Readme.md) to learn how to do this. Once you have, let's assume your CM-Well instance is running at ```localhost:9000```.

### Step 2: Register on PermID.org ###

Browse to the [PermID registration page](https://iamui.thomsonreuters.com/iamui/UI/createUser?app_id=Bold&realm=Bold) and register for free as a PermID.org user, using your email address. You'll have to open your inbox and click a confirmation link.

### Step 3: Download a Bulk File ###

There's a separate bulk file for each type of entity. Browse to [https://permid.org/download](https://permid.org/download) and download the entity bulk file of your choice. For the purpose of this example, I downloaded the Organizations file ```OpenPermID-bulk-organization-20170730_070333.ntriples.gz```, which weighed 384MB.

### Step 4: Ingest the Bulk File into Your CM-Well Instance ###

We'll use one of our data tools for this task: the CM-Well Ingester.

To build the CM-Well data tools:

1. Clone the CM-Well source from [https://github.com/CM-Well/CM-Well](https://github.com/CM-Well/CM-Well).
1. Navigate to the ```server``` directory.
1. Run ```sbt```.
1. At the ```sbt``` prompt, type ```dataToolsApp/pack```. This will pack the Data Tools App into the ```server/cmwell-data-tools-app/target/pack/bin``` directory.

To run the Ingester:

1. Navigate to the bin folder:
   ```$ cd cmwell-data-tools-app/target/pack/bin```
1. Run this command:
   ```./ingester --gzip --format ntriples --file ~/OpenPermID-bulk-organization-20170730_070333.ntriples.gz --host localhost:9000```

During the ingest process, you'll see a live status line, in this format:

    [ingested: 1.3MB][ingested infotons: 836   30.853/sec]   [failed infotons: 0][rate=43.07KB/secaverage rate=109.48KB/sec][12 seconds]

>**Tip:** You can run any CM-Well data tool with the ```--help``` parameter, to learn more about the tool and its input parameters.

*Step 4.5: Have some coffee! The bulk file is large and the ingest can take a few minutes.*

### Step 5: Create the Incremental Update Task ###

So your CM-Well is up-to-date right now, but what about the new updates that PermID.org provides (approximately every 15 minutes)? Once you've downloaded a bulk file, there's no need to do it ever again!

Let's see what a feed looks like. This is how you would access the Organization feed:

    https://permid.org/atom/organization?access-token=<YourAccessToken>&format=ntriples

This returns an atom feed. It has many headers to make it human-readable in a browser, but ultimately you'll want what's in between the ```<summary>``` elements. You can extract this programmatically using your favorite XML Parser, or in bash using xml_grep (a tool in a package available via apt).

We can even pipe the atom stream directly through xml_grep and into the Ingester. The CM-Well data tools support the --file parameter, and when it is not supplied, they read input from STDIN. So our incremental update task can be this one-liner:

    $ curl "https://permid.org/atom/organization?access-token=<YourAccessToken>&format=ntriples" | xml_grep summary --text_only |  ./ingester --format ntriples --host localhost:9000

That's all it takes! Run this command as a scheduled task (e.g. once an hour) on your machine, and you will always be up-to-date with PermID.org data.>

>Note: PermIDs are never deleted, so you don't have to worry about incremental update deleting data. As we mentioned above, every update in PermID.org can be considered as an insert-or-update.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md)

----
