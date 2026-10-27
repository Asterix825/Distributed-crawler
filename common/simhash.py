import re
import xxhash

def compute_simhash(text: str) -> int:
    words = re.findall(r"\w+", text.lower())
    if not words:
        return 0
    
    # Create 3-word sliding shingles
    shingles = [" ".join(words[i:i+3]) for i in range(max(1, len(words) - 2))]
    v = [0] * 64

    for shingle in shingles:
        # 64-bit unsigned integer hash
        h = xxhash.xxh64_intdigest(shingle.encode("utf-8"))
        for i in range(64):
            if (h >> i) & 1:
                v[i] += 1
            else:
                v[i] -= 1

    fingerprint = 0
    for i in range(64):
        if v[i] > 0:
            fingerprint |= (1 << i)

    # Convert unsigned 64-bit to signed 64-bit for Cassandra bigint compatibility
    if fingerprint >= (1 << 63):
        fingerprint -= (1 << 64)
    return fingerprint

def get_pigeonhole_chunks(simhash: int) -> list[tuple[int, int]]:
    u_hash = simhash & 0xFFFFFFFFFFFFFFFF
    return [
        (0, (u_hash >> 48) & 0xFFFF),
        (1, (u_hash >> 32) & 0xFFFF),
        (2, (u_hash >> 16) & 0xFFFF),
        (3, u_hash & 0xFFFF),
    ]

def hamming_distance(hash1: int, hash2: int) -> int:
    x = (hash1 ^ hash2) & 0xFFFFFFFFFFFFFFFF
    return x.bit_count()