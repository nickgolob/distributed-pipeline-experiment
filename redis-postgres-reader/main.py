import psycopg2
import redis
import os
import json
import time

# Environment variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mydatabase")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Connect to Redis
cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST
)
cursor = conn.cursor()

def fetch_messages():
    """Fetch messages with Redis read-through cache."""
    cache_key = "latest_messages"

    # Check cache first
    cached_data = cache.get(cache_key)
    if cached_data:
        print("\n⚡ Cache hit! Returning cached data.")
        return json.loads(cached_data)

    print("\n🐢 Cache miss. Fetching from PostgreSQL...")
    cursor.execute("SELECT * FROM messages ORDER BY id DESC LIMIT 5;")
    rows = cursor.fetchall()

    # Store result in Redis for future requests (cache expires in 10 seconds)
    cache.setex(cache_key, 10, json.dumps(rows))
    
    return rows

print("📥 PostgreSQL Reader with Redis Cache started!")

while True:
    messages = fetch_messages()
    
    print("\n📝 Latest Messages:")
    for row in messages:
        print(row)

    time.sleep(5)  # Fetch every 5 seconds