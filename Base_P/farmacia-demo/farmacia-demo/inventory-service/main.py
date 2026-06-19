"""
Inventory Service - Farmacia Demo
Escucha eventos 'venta.creada' y descuenta stock.
También expone endpoints REST para consultar y reponer stock.
"""
import os, json, asyncio, logging
import aio_pika
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inventory-service")

app = FastAPI(title="Inventory Service", version="1.0")

# ── Configuración ────────────────────────────────────────────────
DB_DSN = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

db_pool = None
rabbit_conn = None

# ── Modelos ──────────────────────────────────────────────────────
class ReponerStock(BaseModel):
    producto_id: int
    cantidad: int

# ── Startup ──────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global db_pool, rabbit_conn
    # Esperar a que la DB esté lista
    for intento in range(15):
        try:
            db_pool = await asyncpg.create_pool(DB_DSN)
            logger.info("✅ Conectado a PostgreSQL (inventario)")
            break
        except Exception as e:
            logger.warning(f"⏳ Esperando DB... intento {intento+1}: {e}")
            await asyncio.sleep(3)

    # Conectar a RabbitMQ
    for intento in range(15):
        try:
            rabbit_conn = await aio_pika.connect_robust(RABBITMQ_URL)
            logger.info("✅ Conectado a RabbitMQ")
            break
        except Exception as e:
            logger.warning(f"⏳ Esperando RabbitMQ... intento {intento+1}: {e}")
            await asyncio.sleep(3)

    # Iniciar consumidor en background
    asyncio.create_task(consumir_eventos())

# ── Consumidor de eventos ────────────────────────────────────────
async def consumir_eventos():
    """Escucha la cola 'venta.creada' y descuenta stock."""
    channel = await rabbit_conn.channel()
    await channel.set_qos(prefetch_count=1)

    exchange = await channel.declare_exchange(
        "farmacia.eventos", aio_pika.ExchangeType.TOPIC, durable=True
    )
    queue = await channel.declare_queue("inventory.venta_creada", durable=True)
    await queue.bind(exchange, routing_key="venta.creada")

    logger.info("📬 Escuchando eventos 'venta.creada'...")

    async with queue.iterator() as msgs:
        async for message in msgs:
            async with message.process():
                data = json.loads(message.body)
                logger.info(f"📩 Evento recibido: {data}")
                await descontar_stock(data)

async def descontar_stock(data: dict):
    producto_id = data["producto_id"]
    cantidad    = data["cantidad"]
    venta_id    = data.get("venta_id", "desconocida")

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            producto = await conn.fetchrow(
                "SELECT * FROM productos WHERE id = $1 FOR UPDATE", producto_id
            )
            if not producto:
                logger.error(f"❌ Producto {producto_id} no encontrado")
                return

            if producto["stock"] < cantidad:
                logger.warning(
                    f"⚠️  Stock insuficiente para producto {producto_id}: "
                    f"tiene {producto['stock']}, se pidió {cantidad}"
                )
                # Registrar en outbox como fallo
                await conn.execute(
                    """INSERT INTO inventory_outbox (evento, payload)
                       VALUES ('stock.insuficiente', $1::jsonb)""",
                    json.dumps({**data, "stock_actual": producto["stock"]})
                )
                return

            nuevo_stock = producto["stock"] - cantidad
            await conn.execute(
                "UPDATE productos SET stock = $1 WHERE id = $2",
                nuevo_stock, producto_id
            )
            # Registrar en outbox (outbox pattern)
            await conn.execute(
                """INSERT INTO inventory_outbox (evento, payload)
                   VALUES ('stock.descontado', $1::jsonb)""",
                json.dumps({
                    "venta_id": venta_id,
                    "producto_id": producto_id,
                    "cantidad_descontada": cantidad,
                    "stock_anterior": producto["stock"],
                    "stock_nuevo": nuevo_stock
                })
            )
            logger.info(
                f"✅ Stock actualizado: producto {producto_id} | "
                f"{producto['stock']} → {nuevo_stock}"
            )

# ── Endpoints REST ───────────────────────────────────────────────
@app.get("/productos")
async def listar_productos():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM productos ORDER BY id")
        return [dict(r) for r in rows]

@app.get("/productos/{producto_id}")
async def obtener_producto(producto_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM productos WHERE id = $1", producto_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        return dict(row)

@app.post("/productos/{producto_id}/reponer")
async def reponer_stock(producto_id: int, body: ReponerStock):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE productos SET stock = stock + $1 WHERE id = $2 RETURNING *",
            body.cantidad, producto_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        return {"mensaje": "Stock repuesto", "producto": dict(row)}

@app.get("/outbox")
async def ver_outbox():
    """Ver eventos registrados (patrón outbox)"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM inventory_outbox ORDER BY creado_en DESC LIMIT 20"
        )
        return [dict(r) for r in rows]

@app.get("/health")
async def health():
    return {"status": "ok", "service": "inventory"}
