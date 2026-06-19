"""
Sales Service - Farmacia Demo
Registra ventas en MongoDB y publica evento 'venta.creada' en RabbitMQ.
Patrón: database-per-service + event publishing
"""
import os, json, asyncio, logging
from datetime import datetime
import aio_pika
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sales-service")

app = FastAPI(title="Sales Service", version="1.0")

MONGO_URL   = os.getenv("MONGO_URL")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

mongo_client = None
db           = None
rabbit_conn  = None
exchange     = None

# ── Modelos ──────────────────────────────────────────────────────
class NuevaVenta(BaseModel):
    producto_id: int
    nombre_producto: str
    cantidad: int
    precio_unitario: float
    sucursal: str
    cliente: str

# ── Startup ──────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global mongo_client, db, rabbit_conn, exchange

    # MongoDB
    for intento in range(15):
        try:
            mongo_client = AsyncIOMotorClient(MONGO_URL)
            await mongo_client.admin.command("ping")
            db = mongo_client["ventas"]
            logger.info("✅ Conectado a MongoDB (ventas)")
            break
        except Exception as e:
            logger.warning(f"⏳ Esperando MongoDB... intento {intento+1}: {e}")
            await asyncio.sleep(3)

    # RabbitMQ
    for intento in range(15):
        try:
            rabbit_conn = await aio_pika.connect_robust(RABBITMQ_URL)
            channel  = await rabbit_conn.channel()
            exchange = await channel.declare_exchange(
                "farmacia.eventos", aio_pika.ExchangeType.TOPIC, durable=True
            )
            logger.info("✅ Conectado a RabbitMQ (sales)")
            break
        except Exception as e:
            logger.warning(f"⏳ Esperando RabbitMQ... intento {intento+1}: {e}")
            await asyncio.sleep(3)

# ── Endpoints ────────────────────────────────────────────────────
@app.post("/ventas", status_code=201)
async def crear_venta(venta: NuevaVenta):
    """
    Registra la venta en MongoDB y publica evento 'venta.creada'.
    NO espera confirmación del inventario (consistencia eventual).
    """
    documento = {
        **venta.model_dump(),
        "total": round(venta.cantidad * venta.precio_unitario, 2),
        "estado": "pendiente",
        "creado_en": datetime.utcnow().isoformat()
    }

    # 1. Guardar en MongoDB (base de datos local del servicio)
    resultado = await db["ventas"].insert_one(documento)
    venta_id  = str(resultado.inserted_id)
    logger.info(f"💾 Venta guardada en MongoDB: {venta_id}")

    # 2. Publicar evento a RabbitMQ
    evento = {
        "venta_id":      venta_id,
        "producto_id":   venta.producto_id,
        "cantidad":      venta.cantidad,
        "sucursal":      venta.sucursal,
        "cliente":       venta.cliente,
        "total":         documento["total"],
        "timestamp":     documento["creado_en"]
    }
    await exchange.publish(
        aio_pika.Message(
            body=json.dumps(evento).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json"
        ),
        routing_key="venta.creada"
    )
    logger.info(f"📤 Evento 'venta.creada' publicado para venta {venta_id}")

    return {
        "mensaje":  "Venta registrada. El inventario se actualizará en breve (consistencia eventual).",
        "venta_id": venta_id,
        "total":    documento["total"]
    }

@app.get("/ventas")
async def listar_ventas():
    ventas = []
    async for doc in db["ventas"].find().sort("creado_en", -1).limit(20):
        doc["_id"] = str(doc["_id"])
        ventas.append(doc)
    return ventas

@app.get("/ventas/{venta_id}")
async def obtener_venta(venta_id: str):
    from bson import ObjectId
    doc = await db["ventas"].find_one({"_id": ObjectId(venta_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    doc["_id"] = str(doc["_id"])
    return doc

@app.get("/health")
async def health():
    return {"status": "ok", "service": "sales"}
