import asyncio, hashlib, io, json, os, time, uuid
from urllib.parse import urljoin
import aiohttp
from aiohttp.resolver import AsyncResolver
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from cassandra.cluster import Cluster
from minio import Minio
from protego import Protego
from selectolax.lexbor import LexborHTMLParser
import valkey.asyncio as valkey
from warcio.warcwriter import WARCWriter
from common.simhash import compute_simhash, get_pigeonhole_chunks, hamming_distance
import os
from dotenv import load_dotenv

class WARCArchiver:
    def __init__(self, minio_client: Minio, bucket: str):
        self.minio = minio_client
        self.bucket = bucket
        os.makedirs("data/warc", exist_ok=True)
        self._rotate()

    def _rotate(self):
        self.filename = f"crawl-{int(time.time())}-{uuid.uuid4().hex[:6]}.warc.gz"
        self.filepath = os.path.join("data/warc", self.filename)
        self.fh = open(self.filepath, "wb")
        self.writer = WARCWriter(self.fh, gzip=True)

    def write(self, url: str, html_bytes: bytes) -> tuple[str, int, int]:
        offset = self.fh.tell()
        record = self.writer.create_warc_record(
            url, "resource", payload=io.BytesIO(html_bytes),
            warc_headers_dict={"Content-Type": "text/html"}
        )
        self.writer.write_record(record)
        self.fh.flush()
        length = self.fh.tell() - offset
        fname = self.filename

        # Rotate at 50MB for local demos (100MB in prod)
        if self.fh.tell() >= 50 * 1024 * 1024:
            self.fh.close()
            self.minio.fput_object(self.bucket, fname, self.filepath)
            os.remove(self.filepath)
            self._rotate()
        return fname, offset, length

async def check_robots(session: aiohttp.ClientSession, vk: valkey.Valkey, host: str, url: str) -> bool:
    key = f"robots:{host}"
    rules_txt = await vk.get(key)
    if rules_txt is None:
        try:
            async with session.get(f"https://{host}/robots.txt", timeout=aiohttp.ClientTimeout(total=5)) as r:
                rules_txt = await r.text() if r.status == 200 else ""
        except Exception:
            rules_txt = ""
        await vk.setex(key, 86400, rules_txt)
    rp = Protego.parse(rules_txt)
    if rp.crawl_delay("*"):
        await vk.setex(f"frontier:delay:{host}", 86400, int(rp.crawl_delay("*") * 1000))
    return rp.can_fetch(url, "PortfolioCrawlerBot")

async def process_url(session, vk, db, archiver, producer, host: str, url: str):
    print(f"[WORKER] Processing -> {url}")
    if not await check_robots(session, vk, host, url):
        return

    try:
        async with session.get(url) as resp:
            if resp.status in (429, 503):
                # Exponential backoff: push host score 60s into the future
                await vk.zadd("frontier:ready_hosts", {host: int(time.time() * 1000) + 60000})
                return
            if resp.status != 200 or "text/html" not in resp.headers.get("Content-Type", "").lower():
                return
            chunks, size = [], 0
            async for chunk in resp.content.iter_chunked(8192):
                size += len(chunk)
                if size > 2 * 1024 * 1024:
                    break
                chunks.append(chunk)
            html_bytes = b"".join(chunks)
    except Exception as e:
        print(f"[FETCH ERROR] {url} -> {repr(e)}")
        return

    # Fast C-based Lexbor DOM extraction
    tree = LexborHTMLParser(html_bytes)
    links = [urljoin(url, n.attributes["href"]) for n in tree.css("a[href]") if n.attributes.get("href")]
    tree.strip_tags(["script", "style", "nav", "footer", "noscript"])
    clean_text = tree.body.text(separator=" ", strip=True) if tree.body else ""

    if links:
        await producer.send_and_wait("discovered_urls", json.dumps(links[:100]).encode("utf-8"))

    sha256 = hashlib.sha256(clean_text.encode("utf-8", errors="replace")).hexdigest()
    simhash = compute_simhash(clean_text)

    # Check Exact Duplicate
    row = db.execute("SELECT canonical_url FROM content_by_sha256 WHERE sha256_hash = %s", (sha256,)).one()
    if row:
        db.execute(
            "INSERT INTO page_metadata (domain, url, http_status, sha256_hash, simhash, is_duplicate, canonical_of) VALUES (%s,%s,200,%s,%s,true,%s)",
            (host, url, sha256, simhash, row.canonical_url)
        )
        print(f"[EXACT DUP] {url} -> {row.canonical_url}")
        return

    # Check Near-Duplicate via 4 Pigeonhole Chunks
    dup_of = None
    for cid, cval in get_pigeonhole_chunks(simhash):
        candidates = db.execute("SELECT simhash, canonical_url FROM simhash_blocks WHERE chunk_id=%s AND chunk_val=%s", (cid, cval))
        for cand in candidates:
            if hamming_distance(simhash, cand.simhash) <= 3:
                dup_of = cand.canonical_url
                break
        if dup_of:
            break

    if dup_of:
        db.execute(
            "INSERT INTO page_metadata (domain, url, http_status, sha256_hash, simhash, is_duplicate, canonical_of) VALUES (%s,%s,200,%s,%s,true,%s)",
            (host, url, sha256, simhash, dup_of)
        )
        print(f"[NEAR DUP (SimHash)] {url} -> {dup_of}")
        return

    # Unique Page: Write WARC & Index Chunks
    warc_file, offset, length = archiver.write(url, html_bytes)
    db.execute(
        "INSERT INTO content_by_sha256 (sha256_hash, canonical_url, warc_file, warc_offset, warc_length) VALUES (%s,%s,%s,%s,%s)",
        (sha256, url, warc_file, offset, length)
    )
    for cid, cval in get_pigeonhole_chunks(simhash):
        db.execute("INSERT INTO simhash_blocks (chunk_id, chunk_val, simhash, canonical_url) VALUES (%s,%s,%s,%s)", (cid, cval, simhash, url))
    db.execute(
        "INSERT INTO page_metadata (domain, url, http_status, sha256_hash, simhash, is_duplicate, warc_file, warc_offset, warc_length) VALUES (%s,%s,200,%s,%s,false,%s,%s,%s)",
        (host, url, sha256, simhash, warc_file, offset, length)
    )
    print(f"[STORED] {url} -> {warc_file} (@{offset} + {length}B)")

load_dotenv()

async def main():
    vk = valkey.Valkey(host="localhost", port=6379, decode_responses=True)
    cluster = Cluster(["127.0.0.1"], port=9042)
    db = cluster.connect("crawler")

    minio_client = Minio(
        "localhost:3900", 
        access_key=os.environ.get("GARAGE_ACCESS_KEY_ID"), 
        secret_key=os.environ.get("GARAGE_SECRET_ACCESS_KEY"),
        region="us-east-1", 
        secure=False)
    if not minio_client.bucket_exists("warc-archive"):
        minio_client.make_bucket("warc-archive")
    archiver = WARCArchiver(minio_client, "warc-archive")

    #resolver = AsyncResolver(nameservers=["127.0.0.1"], udp_port=1053, tcp_port=1053)
    connector = aiohttp.TCPConnector(limit=100, ssl=False)
    timeout = aiohttp.ClientTimeout(total=15, connect=3, sock_read=10)

    consumer = AIOKafkaConsumer("ready_urls", bootstrap_servers="localhost:19092", group_id="worker-group", auto_offset_reset="earliest")
    producer = AIOKafkaProducer(bootstrap_servers="localhost:19092")
    await consumer.start()
    await producer.start()

    print("[WORKER] Connected to Kafka. Listening for URLs...")
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async for msg in consumer:
            payload = json.loads(msg.value.decode("utf-8"))
            await process_url(session, vk, db, archiver, producer, payload["host"], payload["url"])

if __name__ == "__main__":
    asyncio.run(main())