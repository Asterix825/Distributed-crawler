CREATE KEYSPACE IF NOT EXISTS crawler
WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}
AND tablets ={'enabled': false};

USE crawler;

CREATE TABLE IF NOT EXISTS page_metadata (
    domain text,
    url text,
    http_status int,
    sha256_hash text,
    simhash bigint,
    is_duplicate boolean,
    canonical_of text,
    warc_file text,
    warc_offset bigint,
    warc_length int,
    PRIMARY KEY ((domain), url)
);

CREATE TABLE IF NOT EXISTS content_by_sha256 (
    sha256_hash text PRIMARY KEY,
    canonical_url text,
    warc_file text,
    warc_offset bigint,
    warc_length int
);

CREATE TABLE IF NOT EXISTS simhash_blocks (
    chunk_id tinyint,
    chunk_val int,
    simhash bigint,
    canonical_url text,
    PRIMARY KEY ((chunk_id, chunk_val), simhash)
);