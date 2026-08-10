#!/usr/bin/env bash
# Generates the benchmark corpus and loads it into MariaDB.
# Requires: a running MariaDB with local_infile enabled, and ~20 GB free disk.
set -eu

python3 gen.py 30000000 1 logs.tsv
python3 gen.py 5000000 2 metrics.tsv
python3 gen.py 5000000 3 traces.tsv

mysql < schema.sql

load_one() {
    mysql --local-infile=1 bench -e "LOAD DATA LOCAL INFILE '$2' INTO TABLE $1
        (object_id_high,object_id_low,begin_timestamp,end_timestamp,uncompressed_size,size,
         pack_id,expiration_time);"
    echo "loaded $1"
}
load_one archives_logs logs.tsv
load_one archives_logs_nopart logs.tsv
load_one archives_metrics metrics.tsv
load_one archives_traces traces.tsv

# One Pack row per assigned pack_id (K=128, assigned by gen.py in arrival order).
mysql bench -e "INSERT INTO packs_logs (object_id_high,object_id_low,tier,index_configuration_id,
        window_id,num_archives,size,begin_timestamp,end_timestamp,creation_time)
    SELECT FLOOR(RAND(42)*9e18), FLOOR(RAND(43)*9e18), 2, 1,
           FLOOR(MIN(begin_timestamp)/86400000000000),
           COUNT(*), SUM(size), MIN(begin_timestamp), MAX(end_timestamp),
           MAX(end_timestamp)+3600000000000
    FROM archives_logs WHERE pack_id IS NOT NULL GROUP BY pack_id ORDER BY pack_id;"
echo "ALL_LOADED"
