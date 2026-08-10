# Retention cost: dropping one day of data (~82K rows) by partition vs by row-wise DELETE.
# DESTRUCTIVE: removes two days of data from archives_logs; run last.
USE bench;

# Metadata-only, size-independent (6 ms here).
ALTER TABLE archives_logs DROP PARTITION p_20240102;

# Linear in daily volume, undo/purge churn, leaves the tablespace fragmented (1.77 s here).
DELETE FROM archives_logs
WHERE begin_timestamp >= 1704153600000000000 AND begin_timestamp < 1704240000000000000;
