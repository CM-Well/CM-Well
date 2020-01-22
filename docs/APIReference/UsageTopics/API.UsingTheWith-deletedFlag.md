# Using the with-deleted Flag

By default, queries to CM-Well return only the most recent version of each matching infoton (although all versions are retained in the repository). If you add the `with-history` flag, the query returns all "historical" versions as well as the most recent versions. However, deleted infotons are returned neither for the default query nor for a `with-history` query.

The `with-deleted` flag was introduced to allow you to indicate that you wish to retrieve deleted infotons that match the query, as well as "live" infotons.

