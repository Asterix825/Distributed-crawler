from urllib.parse import urlsplit
import asyncio, json, time
import valkey.asyncio as valkey
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from common.normalizer import normalize_url

LUA_POP_AND_SCHEDULE = """
local ready_hosts = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local results = {}
for _, host in ipairs(ready_hosts) do
    local queue_key = 'frontier:queue:' .. host
    local delay_key = 'frontier:delay:' .. host
    local url = redis.call('LPOP', queue_key)
    if url then
        local delay = tonumber(redis.call('GET', delay_key)) or tonumber(ARGV[3])
        redis.call('ZADD', KEYS[1], tonumber(ARGV[1]) + delay, host)
        table.insert(results, cjson.encode({host = host, url = url}))
    else
        redis.call('ZREM', KEYS[1], host)
    end
end
return results
"""

async def ingest_discovered_urls(vk: valkey.Valkey, consumer: AIOKafkaConsumer):
    async for msg in consumer:
        raw_urls = json.loads(msg.value.decode("utf-8"))
        for raw in raw_urls:
            norm = normalize_url(raw)
            if not norm or norm.count("/") > 8:  # Depth trap guard
                continue
            # Atomic Bloom filter check-and-add
            is_new = await vk.execute_command("BF.ADD", "crawler:bloom", norm)
            if is_new == 1:
                print(f"[NEW URL ACCEPTED] -> {norm}")  #test print statement
                host = urlsplit(norm).hostname
                q_len = await vk.rpush(f"frontier:queue:{host}", norm)
                if q_len <= 10000:  # Per-host budget guard
                    now_ms = int(time.time() * 1000)
                    await vk.zadd("frontier:ready_hosts", {host: now_ms}, nx=True)

async def politeness_coordinator(vk: valkey.Valkey, producer: AIOKafkaProducer, script_sha: str):
    print("[FRONTIER] Coordinator started. Waiting for ready hosts...")
    while True:
        now_ms = int(time.time() * 1000)
        popped = await vk.evalsha(script_sha, 1, "frontier:ready_hosts", now_ms, 50, 5000)
        for item_json in popped:
            print(f"[EMITTED to Kafka] -> {item_json}")
            await producer.send_and_wait("ready_urls", item_json.encode("utf-8"))
        await asyncio.sleep(0.1)

async def main():
    vk = valkey.Valkey(host="localhost", port=6379, decode_responses=True)
    try:
        await vk.execute_command("BF.RESERVE", "crawler:bloom", "0.01", "1000000")
    except Exception:
        pass  # Already exists

    script_sha = await vk.script_load(LUA_POP_AND_SCHEDULE)
    producer = AIOKafkaProducer(bootstrap_servers="localhost:19092")
    consumer = AIOKafkaConsumer("discovered_urls", bootstrap_servers="localhost:19092", group_id="frontier-group-v2", auto_offset_reset="earliest")
    await producer.start()
    await consumer.start()

    # Seed initial URLs to kickstart crawling
    #await producer.send_and_wait("discovered_urls", json.dumps(["https://books.toscrape.com/", "https://quotes.toscrape.com/"]).encode())

    # Directly seed Valkey to bypass Kafka offsets and Bloom filters during starting the crawler for the first time. done to tackle the kafka offset issue when restarting the crawler. 
    seeds = ["https://books.toscrape.com/", "https://quotes.toscrape.com/"]
    now_ms = int(time.time() * 1000)
    for seed in seeds:
        host = urlsplit(seed).hostname
        await vk.rpush(f"frontier:queue:{host}", seed)
        await vk.zadd("frontier:ready_hosts", {host: now_ms}, nx=True)

    await asyncio.gather(
        ingest_discovered_urls(vk, consumer),
        politeness_coordinator(vk, producer, script_sha)
    )

if __name__ == "__main__":
    asyncio.run(main())
    