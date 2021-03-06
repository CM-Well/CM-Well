# Mono (November 2017)

-------
New **gqp** query flag | N/A | The new **gqp** flag has the same syntax as **yg**, but only filters the original result set rather than adding inbound/outbound links to it. See [Traversing Outbound and Inbound Links (*xg*, *yg* and *gqp*)](../../APIReference/Traversal/API.Traversal.Intro.md) to learn more.
Play 2.6 | [167](https://github.com/thomsonreuters/CM-Well/issues/167) | Upgraded to Play version 2.6
Prevent deletion/purge of Root infoton | [193](https://github.com/thomsonreuters/CM-Well/issues/193) | Do not allow any form of deletion or purge of the infoton that encapsulates the user information for the root user.
Improvements to consumer | Several | Fixed several bugs that would occur in edge cases.
Bug Fix | [233](https://github.com/thomsonreuters/CM-Well/issues/233) | Improve Authorization Cache mechanism to prevent erroneous 403 errors.
Bug Fix | [245](https://github.com/thomsonreuters/CM-Well/issues/245) | After installing a new Elastic Search version, all ES statuses showed as RED, although all ES services were up and running.


### Changes to API
New **gqp** flag. See [Traversing Outbound and Inbound Links (*xg*, *yg* and *gqp*)](../../APIReference/Traversal/API.Traversal.Intro.md) to learn more.


