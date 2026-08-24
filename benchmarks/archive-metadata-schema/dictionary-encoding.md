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

## 4.0 The break-even, in closed form

Dictionary encoding is not a global win or loss; it is per column, and it is calculable.
With `W` the mean value width, `R` the postings per distinct value, `k` the id width, `A`
the fixed bytes per posting row, `B` the fixed bytes per dictionary row, and `f` the page
fill factor:

    plain = N(A + W)/f
    dict  = N(A + k)/f + (N/R)(B + W)/f

Setting them equal, `f` cancels (same engine, same row format) and `A` cancels (same other
columns, same InnoDB overhead), leaving

        k*R + B
    W* = ───────
         R - 1

The break-even width depends only on the id width and the repetition rate. It can never fall
below `k` -- with unbounded repetition the dictionary is free, so it wins as soon as a value
is wider than its id -- and it DIVERGES as R approaches 1.

`bench_dict_breakeven.py` confirms it, with k=4 and B=30 fitted (raw output in
`results/dict_breakeven_20260824.txt`):

| R   | W\* predicted | observed crossover | bracket                     |
|----:|-------------:|-------------------:|-----------------------------|
| 2   |         38.0 |                ~35 | +1.5% at 32 -> -8.9% at 48  |
| 5   |         12.5 |               ~13.5 | +2.9% at 12 -> -4.9% at 16 |
| 20  |          5.8 |                 <=6 | already -3.3% at 6         |
| 200 |          4.2 |                 <=6 | already -7.1% at 6         |

**Compression raises the break-even, but by no constant factor.** A coarser first run
suggested a clean 2x; the finer grid does not support it -- recomputed ratios are ~1.2,
~1.9, ~1.4 and ">1.8", with no trend in R. Compressed measurements are also visibly noisy,
because zlib's ratio inside an 8 KB block depends on how particular byte patterns land. The
`plain+z` column at R=2 is not even monotonic in width (44.4, 37.4, 32.9, 41.9, 45.4, 52.5,
42.5, 52.0, 61.5 MB for W = 6..64), so +-20% brackets are the best this data supports.

What IS solid about the compressed case:

| repetition R | compressed behaviour                                    |
|-------------:|---------------------------------------------------------|
|            2 | plain wins to 64 B; the dictionary never wins in range   |
|            5 | within a few percent of each other across 12-16 B        |
|          >=20 | the dictionary wins from roughly 10-12 B upward          |

### 4.1 The R = 1 case, and why it is not hypothetical

A file-unique column -- one where each archive carries ~15 values and no value is ever
shared across archives -- has R = 1 exactly. The formula's denominator vanishes: the
dictionary never pays at any width, because each value is stored once in the dictionary AND
referenced once in a posting, where plain storage writes it once in total. The penalty is
`(k + B)/f` per posting, about 53 bytes:

| value width | plain B/row | dict B/row | penalty |
|------------:|------------:|-----------:|--------:|
|        40 B |         131 |        184 |    +40% |
|       100 B |         225 |        278 |    +24% |

Note the opposition: a file-unique column is the BEST case for the side table, since one
value identifies exactly one archive, and the WORST case for a dictionary. No single global
encoding choice is right for both, which is what makes this a per-column decision.

The penalty is nonetheless bounded at 24-40%, not an order of magnitude, so a wrong decision
costs a fraction of one column rather than the design.

---

## 5.0 Recommendation

**For the MVP: take compression, skip the dictionary.**

Compression is one DDL clause, no schema change, no write-path work, and 61% at current
widths. The dictionary at those same widths is a wash, measured directly at the operating
point rather than inferred: `c8` runs W ~ 11.6 with R from 60 to 15,000, and the matching
sweep cells (R=20/W=12 and R=200/W=12) give compressed deltas of **-1.7% and -5.1%**. The
aggregate c8 measurement in section 2.0 put it at -12.2%, higher because that mix includes
columns at R ~ 15,000 where the dictionary does best. Either way it is single digits to low
double digits, at the crossover rather than past it. Against that it costs
five things: a dictionary table, value-to-id resolution with concurrency control on every
write, orphan collection, two-phase prefix wildcards, and per-column encoding decisions --
which means two storage paths and a migration whenever a column changes category.

**If it is built anyway, the mechanism can stay simple.** No online estimators or counters
are needed, because R and W are properties of data already stored. One query after a warm-up
period, once per (dataset, column):

```sql
SELECT column_id,
       COUNT(*) / COUNT(DISTINCT value) AS R,
       AVG(LENGTH(value))               AS W
FROM   side_all
WHERE  dataset_id = ?
GROUP  BY column_id;
```

Encode when `W > 2*(4*R + 30)/(R - 1)`. A file-unique column returns R ~ 1, the threshold is
infinite, and it stays inline with no special case -- the formula already covers it.

**What would change this recommendation:** measuring real filter-value widths above ~20 B.
The `c8` shape's 12 B is a modelling assumption inherited from earlier rounds and is likely
an understatement -- pod names, service identifiers, URLs and error strings land in the
30-80 B range, where the dictionary adds 45-69% on top of compression rather than 12%. That
measurement is one `AVG(LENGTH(value))` over real datasets, and this entire decision hinges
on it, so it should come before any implementation.

Separately, the wildcard and key-limit arguments (section 3.0) may justify the dictionary
even at narrow widths, since dissolving the oversized-value machinery removes a subsystem
rather than optimizing one. That is a design-simplicity argument, not a storage one, and
should be weighed on its own terms.

---

## 6.0 Open questions

1. **Real value widths.** Everything in section 4.0 turns on this and it is unmeasured.
2. **Query cost of the extra lookup.** One additional index seek by inspection; unmeasured.
3. **Ingest cost of id resolution**, including contention when many writers insert the same
   new value concurrently. Unmeasured.
4. **Prefix-wildcard regression.** The two-phase form could be materially worse when a
   prefix matches many distinct values; the crossover point is unmeasured.
5. Whether the dictionary should be per-dataset or deployment-global -- the same question
   raised for `column_id` numbering in the topology document.
