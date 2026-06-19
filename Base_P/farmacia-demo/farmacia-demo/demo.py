#!/usr/bin/env python3
"""
=============================================================
  DEMO EXPOSICIÓN - TEMA 9: Microservicios y Patrones de Datos
  IS-404 Administración de Bases de Datos Distribuidas | ULEAM 2026-1
=============================================================
  Ejecutar DESPUÉS de levantar docker-compose up -d
  Uso: python3 demo.py
"""
import requests, json, time

BASE_INVENTORY = "http://localhost:8001"
BASE_SALES     = "http://localhost:8002"
BASE_AUDIT     = "http://localhost:8003"

def separador(titulo: str):
    print("\n" + "="*60)
    print(f"  {titulo}")
    print("="*60)

def paso(n: int, descripcion: str):
    print(f"\n[PASO {n}] {descripcion}")
    print("-" * 50)

# ─────────────────────────────────────────────────────────────
# PASO 1: Verificar salud de los tres servicios
# ─────────────────────────────────────────────────────────────
separador("PASO 1 — Verificar que los 3 microservicios están activos")
for nombre, url in [("Inventory", BASE_INVENTORY),
                    ("Sales",     BASE_SALES),
                    ("Audit",     BASE_AUDIT)]:
    try:
        r = requests.get(f"{url}/health", timeout=5)
        print(f"   {nombre}: {r.json()}")
    except Exception as e:
        print(f"   {nombre}: {e}")

input("\n  Presiona ENTER para continuar al Paso 2...")

# ─────────────────────────────────────────────────────────────
# PASO 2: Ver stock inicial en inventario
# ─────────────────────────────────────────────────────────────
separador("PASO 2 — Stock inicial de productos (PostgreSQL - Inventory DB)")
r = requests.get(f"{BASE_INVENTORY}/productos")
productos = r.json()
print(f"  {'ID':<5} {'Producto':<25} {'Stock':<8} {'Precio':<8} {'Sucursal'}")
print(f"  {'-'*65}")
for p in productos:
    print(f"  {p['id']:<5} {p['nombre']:<25} {p['stock']:<8} {p['precio']:<8} {p['sucursal']}")

input("\n  Presiona ENTER para continuar al Paso 3...")

# ─────────────────────────────────────────────────────────────
# PASO 3: Registrar una venta
# ─────────────────────────────────────────────────────────────
separador("PASO 3 — Registrar venta de Paracetamol (Sales Service → MongoDB)")
print("   Enviando POST /ventas al Sales Service...")
print("   El servicio NO espera al inventario (consistencia eventual)")

venta = {
    "producto_id":     1,
    "nombre_producto": "Paracetamol 500mg",
    "cantidad":        10,
    "precio_unitario": 0.50,
    "sucursal":        "Guayaquil-Norte",
    "cliente":         "Carlos Mendoza"
}
print(f"\n  Payload enviado:\n  {json.dumps(venta, indent=4, ensure_ascii=False)}")

r = requests.post(f"{BASE_SALES}/ventas", json=venta)
resultado = r.json()
venta_id  = resultado.get("venta_id")
print(f"\n  Respuesta del Sales Service:\n  {json.dumps(resultado, indent=4, ensure_ascii=False)}")
print(f"\n   Venta ID: {venta_id}")

input("\n  Presiona ENTER para continuar al Paso 4...")

# ─────────────────────────────────────────────────────────────
# PASO 4: Ver que el stock YA fue descontado (async)
# ─────────────────────────────────────────────────────────────
separador("PASO 4 — Verificar que el inventario se actualizó (asíncrono)")
print("   Esperando 2 segundos para que el evento se procese...")
time.sleep(2)

r = requests.get(f"{BASE_INVENTORY}/productos/1")
producto = r.json()
print(f"\n  Producto 1 - Paracetamol 500mg:")
print(f"    Stock anterior: 100")
print(f"    Stock actual:   {producto['stock']}  ← se descontaron 10 unidades")
print(f"\n   El inventario se actualizó SIN llamada directa entre servicios.")
print(f"   Comunicación por eventos (RabbitMQ), no por HTTP.")

input("\n  Presiona ENTER para continuar al Paso 5...")

# ─────────────────────────────────────────────────────────────
# PASO 5: Ver el outbox del inventario (patrón outbox)
# ─────────────────────────────────────────────────────────────
separador("PASO 5 — Outbox Pattern (garantía de entrega)")
r = requests.get(f"{BASE_INVENTORY}/outbox")
eventos_outbox = r.json()
print(f"  Eventos registrados en inventory_outbox (PostgreSQL):\n")
for ev in eventos_outbox:
    print(f"  [{ev['id']}] {ev['evento']} | {ev['payload']}")

input("\n  Presiona ENTER para continuar al Paso 6...")

# ─────────────────────────────────────────────────────────────
# PASO 6: Simular fallo — stock insuficiente
# ─────────────────────────────────────────────────────────────
separador("PASO 6 — Simular fallo: intentar vender más stock del disponible")
venta_fallida = {
    "producto_id":     1,
    "nombre_producto": "Paracetamol 500mg",
    "cantidad":        999,   # más del stock disponible
    "precio_unitario": 0.50,
    "sucursal":        "Guayaquil-Norte",
    "cliente":         "Test Fallo"
}
print(f"   Enviando venta de 999 unidades (stock actual ~90)...")
r = requests.post(f"{BASE_SALES}/ventas", json=venta_fallida)
resultado_fallo = r.json()
print(f"  Sales Service acepta la venta (aún no sabe del stock):")
print(f"  {json.dumps(resultado_fallo, indent=4, ensure_ascii=False)}")

print("\n   Esperando 2 segundos para que el evento se procese...")
time.sleep(2)

# Ver outbox — debe aparecer 'stock.insuficiente'
r = requests.get(f"{BASE_INVENTORY}/outbox")
print(f"\n  Outbox actualizado:")
for ev in r.json():
    icono = " " if ev["evento"] == "stock.insuficiente" else ""
    print(f"  {icono} [{ev['id']}] {ev['evento']}")

print("\n    El inventario registró el fallo en su outbox.")
print("  En producción: un proceso de compensación (SAGA) revertiría la venta.")

input("\n  Presiona ENTER para continuar al Paso 7...")

# ─────────────────────────────────────────────────────────────
# PASO 7: Auditoría completa
# ─────────────────────────────────────────────────────────────
separador("PASO 7 — Trazabilidad completa en el Audit Service")
r = requests.get(f"{BASE_AUDIT}/auditoria")
registros = r.json()
print(f"  Registros en audit_log (PostgreSQL - Audit DB):\n")
print(f"  {'Evento':<25} {'Venta ID':<28} {'Prod':<6} {'Total'}")
print(f"  {'-'*75}")
for reg in registros:
    print(
        f"  {str(reg['evento']):<25} "
        f"{str(reg['venta_id'] or '-'):<28} "
        f"{str(reg['producto_id'] or '-'):<6} "
        f"{reg['total'] or '-'}"
    )

print(f"\n  Resumen por tipo de evento:")
r2 = requests.get(f"{BASE_AUDIT}/auditoria/resumen")
for item in r2.json():
    print(f"    • {item['evento']}: {item['total']} registro(s)")

input("\n  Presiona ENTER para continuar al Paso 8...")

# ─────────────────────────────────────────────────────────────
# PASO 8: Ver ventas en MongoDB
# ─────────────────────────────────────────────────────────────
separador("PASO 8 — Ventas almacenadas en MongoDB (Sales DB)")
r = requests.get(f"{BASE_SALES}/ventas")
ventas = r.json()
for v in ventas:
    print(f"   [{v['_id']}]")
    print(f"     Producto:  {v['nombre_producto']} x{v['cantidad']}")
    print(f"     Cliente:   {v['cliente']} | Sucursal: {v['sucursal']}")
    print(f"     Total:     ${v['total']} | Estado: {v['estado']}")
    print()

# ─────────────────────────────────────────────────────────────
# RESUMEN FINAL
# ─────────────────────────────────────────────────────────────
separador("RESUMEN — Patrones demostrados")
patrones = [
    ("Database-per-Service", "Cada microservicio tiene su propia base de datos"),
    ("Event-Driven",         "Sales → RabbitMQ → Inventory/Audit (sin HTTP directo)"),
    ("Outbox Pattern",       "Inventory registra eventos en su BD antes de publicar"),
    ("Consistencia Eventual","Sales responde inmediato; inventario actualiza después"),
    ("SAGA (Compensación)",  "Stock insuficiente → evento de fallo para reversa futura"),
    ("Observabilidad",       "Audit Service captura 100% de eventos con routing_key=#"),
]
for patron, desc in patrones:
    print(f"   {patron:<22} → {desc}")

print("\n   Recomendación para cadena de farmacias en Ecuador:")
print("     Usar este patrón para: ventas, recetas, inventario por sucursal.")
print("     Cada sucursal puede operar offline y sincronizar por eventos.")
print("     La auditoría centralizada cumple con la LOPDP Ecuador.\n")
