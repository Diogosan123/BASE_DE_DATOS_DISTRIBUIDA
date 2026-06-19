"""
Audit Service - Farmacia Demo
Suscribe a TODOS los eventos de farmacia y los registra en PostgreSQL.
Demuestra: trazabilidad multi-servicio sin acoplamiento directo.
"""
import os, json, asyncio, logging
import aio_pika
import asyncpg
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("audit-service")

app = FastAPI(title="Audit Service", version="1.0")

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

db_pool     = None
rabbit_conn = None

# ── Startup ──────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global db_pool, rabbit_conn

    for intento in range(15):
        try:
            db_pool = await asyncpg.create_pool(DB_DSN)
            logger.info("✅ Conectado a PostgreSQL (auditoría)")
            break
        except Exception as e:
            logger.warning(f"⏳ Esperando DB auditoria... {intento+1}: {e}")
            await asyncio.sleep(3)

    for intento in range(15):
        try:
            rabbit_conn = await aio_pika.connect_robust(RABBITMQ_URL)
            logger.info("✅ Conectado a RabbitMQ (audit)")
            break
        except Exception as e:
            logger.warning(f"⏳ Esperando RabbitMQ audit... {intento+1}: {e}")
            await asyncio.sleep(3)

    asyncio.create_task(consumir_todos_los_eventos())

# ── Consumidor wildcard ──────────────────────────────────────────
async def consumir_todos_los_eventos():
    """Escucha '#' → captura absolutamente todos los eventos del exchange."""
    channel  = await rabbit_conn.channel()
    await channel.set_qos(prefetch_count=1)

    exchange = await channel.declare_exchange(
        "farmacia.eventos", aio_pika.ExchangeType.TOPIC, durable=True
    )
    queue = await channel.declare_queue("audit.todos_eventos", durable=True)
    await queue.bind(exchange, routing_key="#")   # wildcard: todo

    logger.info("📬 Auditando TODOS los eventos (routing_key=#)...")

    async with queue.iterator() as msgs:
        async for message in msgs:
            async with message.process():
                evento  = message.routing_key
                payload = json.loads(message.body)
                logger.info(f"📝 Auditando evento '{evento}': {payload}")
                await registrar_auditoria(evento, payload)

async def registrar_auditoria(evento: str, payload: dict):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO audit_log
               (evento, venta_id, producto_id, sucursal, cliente, cantidad, total, payload)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)""",
            evento,
            payload.get("venta_id"),
            payload.get("producto_id"),
            payload.get("sucursal"),
            payload.get("cliente"),
            payload.get("cantidad"),
            payload.get("total"),
            json.dumps(payload)
        )

# ── Endpoints ────────────────────────────────────────────────────
@app.get("/auditoria")
async def listar_auditoria():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM audit_log ORDER BY recibido_en DESC LIMIT 50"
        )
        return [dict(r) for r in rows]

@app.get("/auditoria/por-venta/{venta_id}")
async def auditoria_por_venta(venta_id: str):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM audit_log WHERE venta_id = $1 ORDER BY recibido_en",
            venta_id
        )
        return [dict(r) for r in rows]

@app.get("/auditoria/resumen")
async def resumen():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT evento, COUNT(*) as total FROM audit_log GROUP BY evento ORDER BY total DESC"
        )
        return [dict(r) for r in rows]

@app.get("/health")
async def health():
    return {"status": "ok", "service": "audit"}
