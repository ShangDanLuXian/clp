# E7: online schema evolution with the indexed VIRTUAL is_long_span column (MariaDB).
# The proposal adds lc_* columns online at index-configuration updates, so INSTANT matters.
USE bench;

# On the proposed table (VIRTUAL is_long_span, leading archives_time_lookup) this fails with
# ERROR 1846: ALGORITHM=INSTANT is not supported. Reason: online rebuild with indexed virtual
# columns. Try ALGORITHM=INPLACE.
ALTER TABLE archives_logs ADD COLUMN lc_varchar_f02 VARCHAR(256) NULL, ALGORITHM=INSTANT;

# Controls isolating the cause. ctrl: VIRTUAL generated column, index added separately below.
DROP TABLE IF EXISTS ctrl, ctrl2;
CREATE TABLE ctrl (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    begin_timestamp BIGINT NOT NULL,
    end_timestamp BIGINT NOT NULL,
    is_long_span BOOLEAN GENERATED ALWAYS AS
        ((end_timestamp - begin_timestamp) > 86400000000000) VIRTUAL,
    PRIMARY KEY (id, begin_timestamp)
) ENGINE=InnoDB
PARTITION BY RANGE (begin_timestamp) (PARTITION p0 VALUES LESS THAN MAXVALUE);
INSERT INTO ctrl (begin_timestamp, end_timestamp) VALUES (1, 2), (3, 4);

# Control 1: VIRTUAL column present but UNINDEXED -- INSTANT works.
ALTER TABLE ctrl ADD COLUMN x1 VARCHAR(64) NULL, ALGORITHM=INSTANT;

# Control 2: index the VIRTUAL column -- INSTANT is now refused (ERROR 1846).
ALTER TABLE ctrl ADD KEY i1 (is_long_span, begin_timestamp);
ALTER TABLE ctrl ADD COLUMN x2 VARCHAR(64) NULL, ALGORITHM=INSTANT;

# Control 3: same shape with STORED generated column, indexed -- INSTANT works again.
CREATE TABLE ctrl2 (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    begin_timestamp BIGINT NOT NULL,
    end_timestamp BIGINT NOT NULL,
    is_long_span BOOLEAN GENERATED ALWAYS AS
        ((end_timestamp - begin_timestamp) > 86400000000000) STORED,
    x1 VARCHAR(64) NULL,
    PRIMARY KEY (id, begin_timestamp),
    KEY i1 (is_long_span, begin_timestamp)
) ENGINE=InnoDB
PARTITION BY RANGE (begin_timestamp) (PARTITION p0 VALUES LESS THAN MAXVALUE);
INSERT INTO ctrl2 (begin_timestamp, end_timestamp) VALUES (1, 2), (3, 4);
ALTER TABLE ctrl2 ADD COLUMN x2 VARCHAR(64) NULL, ALGORITHM=INSTANT;

# The forced fallback on the 30M-row table is a full-table-copy rebuild: needs table-sized
# free disk and took 2m01s idle here (see results/e7_inplace2.log). Run it timed from a shell:
#   time mysql bench -e \
#     "ALTER TABLE archives_logs ADD COLUMN lc_varchar_f02 VARCHAR(256) NULL,
#      ALGORITHM=INPLACE;"
