# Testing Your CM-Well Application #

Before deploying a new CM-Well project, you'll need to test both your own application's functionality and its interaction with CM-Well, in a way that simulates operational conditions. You and your QA team are responsible for performing all necessary types of testing. The TMS QA team will provide all the support you need to ensure the success of your project.

This page describes the various aspects of testing a CM-Well client application.

## Testing Environments ##

Some CM-Well projects are deployed in private environments that are used only by the specific customer. If this is your case, test your project within your own private environment.

The alternative is to use the "public" (within TR) CM-Well environments. The following table describes the different public CM-Well environments and when you should use each one:

Environment | Use for:
:-----------|:---------
Lab | Initial development and functional debugging
PPE (Pre-Production Environment) | Non-functional testing (load, stress, data consistency...)
Production | Operational deployment after all testing is completed successfully

## Types of Tests ##

Test Type | Description | Test in:
:---------|:------------|:--------
Error recovery | If you anticipate that you may need to perform error corrections on your data, such as deletions of infotons or fields, test these actions. | Lab
Data correctness | If your application writes data to CM-Well, examine the different types of infotons it writes in a browser or other tool, to verify that data is written as intended. | Lab
Data consistency | Run 2 clients: one to generate constant load, and one to repeat the same query at regular intervals. Make sure the data returned is complete and consistent for each query. | PPE
Stress | Run a client that simulates your expected peak load (rate of calls to CM-Well). Verify that all calls succeed. | PPE
Load | Simulate the expected load at peak times, for 12-24 hours. | PPE
Latency | Run a client at your expected peak load, and verify that call latency (the time it takes to get a response) meets your operational needs. | PPE
Long-term | Run your client for 3-7 days at operational load and make sure there is no degradation in CM-Well calls' success and latency, or increase in your application's CPU and memory usage. | PPE

>**Note:** Before beginning any kind of performance testing in a CM-Well environment, please notify the <a href="mailto:yaniv.yemini@thomsonreuters.com">TMS Deployment & Operations team</a>

## Coordinating with the TMS Team ##

Engaging with the TMS project, development and QA teams early and often will enable your project's successful and timely completion. See [TMS Engagement Milestones](ADDLINK) to learn more about the engagement milestones for a CM-Well project.

Specifically, when you want to move to the PPE or Prod environments, this must be coordinated with the <a href="mailto:yaniv.yemini@thomsonreuters.com">TMS Deployment & Operations team</a>. It's also a good idea to discuss your testing plans with the TMS QA team, to ensure your success and avoid bad practices that may impact other CM-Well users.

## Tools for Long-Term Testing ##

During testing, you may want to use these "off-the-shelf" tools to simulate operational read/write load:

* [CM-Well Downloader](Tools.UsingTheCM-WellDownloader.md) - a stand-alone tool for bulk download from CM-Well.
* [CM-Well Ingester](Tools.UsingTheCM-WellIngester.md) - a stand-alone tool for bulk upload to CM-Well.


