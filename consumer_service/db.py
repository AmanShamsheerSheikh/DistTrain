from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from asyncpg import Pool
import asyncpg
from settings import kafka_settings, pg_settings
import modal


consumer: AIOKafkaConsumer | None = None
db_pool: Pool | None = None
gpu_worker = None
producer: AIOKafkaProducer | None = None

async def init():
    global consumer, db_pool, gpu_worker, producer
    consumer = AIOKafkaConsumer(
        kafka_settings.KAFKA_INFERENCE_TOPIC,
        group_id=kafka_settings.KAFKA_GROUP_ID,
        bootstrap_servers=kafka_settings.KAFKA_BOOTSTRAP_SERVER,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed"
    )
    db_pool = await asyncpg.create_pool(
        user=pg_settings.POSTGRES_USER,
        password=pg_settings.POSTGRES_PASSWORD,
        host=pg_settings.POSTGRES_HOST,
        port=pg_settings.PGBOUNCER_PORT,
        database=pg_settings.POSTGRES_DB,
        min_size=pg_settings.PGBOUNCER_MIN_CONNECTIONS,
        max_size=pg_settings.PGBOUNCER_MAX_CONNECTIONS,
        statement_cache_size=0
    )
    await consumer.start()
    gpu_worker = modal.Cls.from_name("distserve-gpu-worker", "GPUWorker")()
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_settings.KAFKA_BOOTSTRAP_SERVER,
        transactional_id="distserve-producer-1",
        enable_idempotence=True,
        retry_backoff_ms=200,
        transaction_timeout_ms=60000,
        request_timeout_ms=40000
    )
    producer.start()

def get_consumer():
    return consumer

def get_db_pool():
    return db_pool

def get_gpu_worker():
    return gpu_worker

def get_producer():
    return producer