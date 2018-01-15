# diachronic

A parser that turns the revision history dump for a set of wiki sites 
(e.g. Wikipeida, Wiktionary) into parquet files of daily snapshots.

Uses Apache Arrow for serialization.

The files are uploaded to a specified Google Cloud bucket.