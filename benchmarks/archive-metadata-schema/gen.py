"""Generates archive rows: 2024 arrivals, span lognormal-ish (median ~10min, p99 < 1 day),
0.1% long-span (2-30 days). pack_id assigned K=128 in arrival order; newest 2% unpacked."""
import random, sys, math
rows, seed, out = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
rng = random.Random(seed)
T0 = 1704067200_000000000            # 2024-01-01 UTC ns
YEAR = 366*86400*1_000_000_000
DAY = 86400*1_000_000_000
K = 128
packed_until = int(rows*0.98)
with open(out,"w") as f:
    step = YEAR / rows
    for i in range(rows):
        begin = T0 + int(i*step) + rng.randrange(0, max(1,int(step)))
        if rng.random() < 0.001:
            span = rng.randrange(2*DAY, 30*DAY)          # long-span
        else:
            span = min(int(math.exp(rng.gauss(6.4,1.5))*1e9), DAY-1)  # median ~10min
        end = begin + span
        pack = (i // K) + 1 if i < packed_until else "\\N"
        exp_s = (begin // 1_000_000_000) + 86400*400     # all unexpired at snapshot
        f.write(f"{rng.getrandbits(63)}\t{rng.getrandbits(63)}\t{begin}\t{end}\t"
                f"{rng.randrange(200_000_000,300_000_000)}\t{rng.randrange(6_000_000,12_000_000)}\t"
                f"{pack}\t{exp_s}\n")
print("generated", rows, file=sys.stderr)
