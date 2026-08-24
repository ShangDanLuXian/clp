# Dictionary-encoded values in the side table

The side table stores one row per `(column, value, archive)` posting. Within one dataset a
filter column draws from a small, stable set of values, so the same bytes are stored once
per posting -- hundreds of times over. This document measures the alternative: a per-dataset
dictionary mapping value to a fixed-width id, with the side table holding the id.

    plain   PRIMARY KEY (dataset_id, column_id, value,    begin_timestamp, archive_id)

    dict    PRIMARY KEY (dataset_id, column_id, value_id, begin_timestamp, archive_id)
            + dict: (dataset_id, column_id, value) -> value_id

Measured by `bench_dict.py`; raw output in `results/dict_encoding_20260824.txt`.

---

## 1.0 Why this could not be estimated

Stored bytes per row is `(payload + per-row overhead) / page fill factor`. The topology run
gives one number -- 80.9 stored bytes against 26.6 bytes of payload -- which fixes the
product of overhead and fill factor but not either one. Since the overhead is identical in
both designs and the fill factor is not necessarily so, an estimate would have had to assume
the answer. Both schemas were therefore built with byte-identical logical data and measured.

Value width is swept because the saving is bounded by the fraction of the row the value
occupies, and that fraction is small at the `c8` shape's ~12 B and large at the 30-80 B
values real deployments carry (pod names, service identifiers, URLs, error strings).
Compression is swept because the primary key sorts BY value, so identical values are
adjacent and a page holds few distinct ones -- page compression should already remove much
of the redundancy dictionary encoding targets.

---

## 2.0 Results

2,700,000 postings (60,000 archives x 45), 3,404 distinct values, MariaDB 10.11. Every
variant holds identical rows; only the value encoding differs. The `dict` figures INCLUDE
the dictionary table, so the comparison is net.

| value width | design                   |    MB | B/row | vs plain |
|------------:|--------------------------|------:|------:|---------:|
|        12 B | plain (value inline)     | 226.0 |  87.8 |       -- |
|        12 B | dict (INT value_id)      | 186.2 |  72.3 |   -17.6% |
|        12 B | plain + COMPRESSED kbs8  |  88.5 |  34.4 |   -60.8% |
|        12 B | dict + COMPRESSED kbs8   |  77.7 |  30.2 |   -65.6% |
|        40 B | plain (value inline)     | 346.0 | 134.4 |       -- |
|        40 B | dict (INT value_id)      | 186.3 |  72.4 |   -46.2% |
|        40 B | plain + COMPRESSED kbs8  | 141.0 |  54.8 |   -59.2% |
|        40 B | dict + COMPRESSED kbs8   |  77.8 |  30.2 |   -77.5% |
|       100 B | plain (value inline)     | 582.0 | 226.0 |       -- |
|       100 B | dict (INT value_id)      | 187.5 |  72.8 |   -67.8% |
|       100 B | plain + COMPRESSED kbs8  | 258.0 | 100.2 |   -55.7% |
|       100 B | dict + COMPRESSED kbs8   |  79.0 |  30.7 |   -86.4% |

### 2.1 Dictionary encoding decouples side-table size from value width

Read down the `dict` rows: 186.2, 186.3, 187.5 MB as values grow from 12 B to 100 B.
Compressed: 77.7, 77.8, 79.0. The side table's rows are fixed-width, so a wider value costs
0.4 MB more in the dictionary instead of 356 MB more in the postings. The plain design
nearly triples over the same range.

This is the result that matters most, because it changes what the storage-admission policy
has to price. Under `plain`, per-archive payload depends on value width, so the 0.1% budget
must charge for wide values. Under `dict`, the side-table cost per posting is constant and
the budget becomes a count of postings, with value width nearly free.

### 2.2 At `c8` widths, compression is the larger single lever

17.6% for the dictionary against 60.8% for compression, and only 12.2% for the dictionary
ON TOP of compression. The two are near-substitutes at 12 B -- which is what the adjacency
argument predicts, since a page at that width holds one or two distinct values repeated.

That equivalence does not survive wider values:

| value width | dict's marginal gain AFTER compression |
|------------:|---------------------------------------:|
|        12 B |                                  12.2% |
|        40 B |                                  44.8% |
|       100 B |                                  69.4% |

Compression's marginal gain after the dictionary, by contrast, is flat at ~58% for every
width. So compression is unconditionally worth taking; the dictionary's value is entirely a
function of how wide real filter values turn out to be.

### 2.3 Effect on the storage projections

At the `c8` shape, `dict + COMPRESSED` is 34.4% of plain:

| projection                              | plain    | dict + compressed |
|-----------------------------------------|---------:|------------------:|
| topology run's side table               | 4,166 MB |         ~1,430 MB |
| 1 user, 7 years, at `c8`                |    ~1 TB |           ~344 GB |
| 1 user, 7 years, at the `wide` policy cap |   ~9 TB |           ~3.1 TB |

---

## 3.0 The consequences that are not about size

**It removes the value from the side table's primary key**, and with it the entire
oversized-value apparatus: `VARBINARY(1024)`, the 3,059-byte index-key ceiling, the overflow
table, and the per-archive rejection valve all exist because the value sits inside a key
that is written once per posting. With `value_id INT` the key is a fixed 19 bytes with no
length limit. The length problem moves to the dictionary, where a long value is stored ONCE
rather than once per posting -- and it can be removed there too, by keying the dictionary on
`UNIQUE (dataset_id, column_id, value_hash)` and holding the full value in an unindexed
BLOB. Exact-match lookup is then a hash probe with no length limit anywhere.

**Infix wildcards scan distinct values instead of postings.** `value LIKE '%abc%'` currently
scans every posting for the column; against a dictionary it scans the distinct set. The
ratio is that column's postings-per-value, so it varies widely:

| column        | distinct | postings/dataset | rows scanned |
|---------------|---------:|-----------------:|-------------:|
| `h0` host     |    1,000 |           12,000 |    12x fewer |
| `m0` module   |      300 |          240,000 |   800x fewer |
| `e0` env      |       12 |           36,000 | 3,000x fewer |

The gain is smallest on the columns closest to high cardinality and largest on the genuinely
low-cardinality ones, which is the right direction for an index designed around the latter.

**Runaway cardinality becomes directly observable.** A column whose dictionary grows past a
threshold is not low-cardinality, which is a cleaner demotion signal than counting postings.

### 3.1 Costs

- **Value-to-id resolution on every write.** An ingester-side cache plus `INSERT IGNORE`
  handles it; concurrent inserts of a new value need the unique key to arbitrate. At the
  design ingest rate this is ~90 lookups/second, nearly all cache hits.
- **Prefix wildcards become two-phase.** `abc%` is one range scan today; against a dictionary
  it is a dictionary range scan producing an id list, then one probe per id. Worse when the
  prefix matches many values.
- **Orphaned dictionary entries** after retention deletes. The table is bounded by distinct
  values, so leaking is acceptable; a periodic sweep is optional.

---

## 4.0 Recommendation

Take compression now: one DDL clause, no schema change, no write-path work, 61% at current
widths.

Treat the dictionary as a separate decision that hinges on a fact not yet established --
**the width distribution of real filter values.** The `c8` shape's 12 B is a modelling
assumption inherited from earlier rounds, and it is likely an understatement: pod names,
service identifiers, URLs and error strings land in the 30-80 B range, where the dictionary
adds 45-69% on top of compression rather than 12%.

Measure that distribution before deciding. If values really are ~12 B, the dictionary buys
little space and should be justified on the wildcard and key-limit arguments alone -- which
may well be enough, since dissolving the oversized-value machinery removes a subsystem
rather than optimizing one.

---

## 5.0 Open questions

1. **Real value widths.** Everything in section 4.0 turns on this and it is unmeasured.
2. **Query cost of the extra lookup.** One additional index seek by inspection; unmeasured.
3. **Ingest cost of id resolution**, including contention when many writers insert the same
   new value concurrently. Unmeasured.
4. **Prefix-wildcard regression.** The two-phase form could be materially worse when a
   prefix matches many distinct values; the crossover point is unmeasured.
5. Whether the dictionary should be per-dataset or deployment-global -- the same question
   raised for `column_id` numbering in the topology document.
