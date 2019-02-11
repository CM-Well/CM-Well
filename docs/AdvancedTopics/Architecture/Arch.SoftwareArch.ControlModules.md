# Control Modules

## Health Control (HC) Agent

The Health Control (HC) agent is a service (run in the DC JVM) that runs on each CM-Well machine. The HC is responsible for sensing and reacting to the state of other CM-Well processes on its machine. If the HC agent identifies that a CM-Well process has become unresponsive or crashed, it kills it/restarts it.

In addition, each HC collect report data about the health of the components running on its machine. A single HC is elected as the central reporter for the platform. It aggregates health data from the other HCs. You can view the aggregated health data of the entire CM-Well grid via CM-Well's web interface (see [Health Monitoring Pages](Arch.SoftwareArch.WebApplication.md#HealthMonitoring) to learn more).

## DC JVM

Several services have Dedicated Container JVMs (DC JVMs), for example, Health Control, DC-Sync, and STP. These are monitored by the grid-level Controller JVM, and can safely be killed and restarted in case of hangs, crashes or other errors situations.

## Controller JVM

The Controller JVM is a grid-level service which is used to manage Akka clusters and other core grid services as Dedicated Container JVMs (see above). There is a single instance of the Controller JVM per data center.