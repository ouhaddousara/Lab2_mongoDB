\# Technical Notes \& Design Decisions



\## Why PyMongo over Motor (async)?

This lab runs sequential operations on a single thread — synchronous PyMongo

is simpler and sufficient. Motor would add complexity without benefit here.



\## Why bulk\_write() for sentiment enrichment (Q5)?

Instead of 200 separate `update\_one()` calls (200 network round-trips),

`bulk\_write()` sends all updates in a single batch → \~50x faster.



\## Why allowDiskUse=True in aggregations?

MongoDB limits in-memory aggregation to 100MB. The sample\_mflix dataset

with $lookup and $group operations can exceed this — this flag prevents

silent failures in production.



\## Why validationAction="error" over "warn" (Q7)?

"warn" lets invalid documents pass silently → corrupts downstream pipelines.

"error" rejects them immediately → data integrity guaranteed.



\## Why validationLevel="strict" (Q7)?

"strict" validates ALL inserts AND updates.

"moderate" only validates new inserts → leaves room for dirty updates.



\## Why get\_db() as a centralized function?

Single point of configuration → changing clusters requires modifying

one line instead of searching across the entire codebase.



\## Why TEXT index on (title, plot) (Q1)?

Enables MongoDB's $text operator for full-text search.

Without it → full collection scan on 21,349 documents per query.

With it → O(log n) lookup → dramatically faster search.



\## Why compound DESCENDING index on (year, imdb.rating) (Q1)?

Covers queries like "top-rated films from year X" with a single index scan.

DESCENDING order matches the most common sort pattern (newest/highest first).



\## Why datetime() for released field (Q2)?

MongoDB stores dates as BSON Date — not strings.

Using datetime() enables $year, $month operators in aggregation pipelines (Q6).

String dates would break all date-based aggregations.



\## SSL Issue on Windows

MongoDB Atlas uses TLS 1.2+. Some Windows networks (university, corporate)

block port 27017 or intercept SSL → TLSV1\_ALERT\_INTERNAL\_ERROR.

Fix: add current IP to Atlas Network Access whitelist.

