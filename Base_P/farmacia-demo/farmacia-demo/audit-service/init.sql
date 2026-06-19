-- Tabla principal de auditoría de eventos
CREATE TABLE IF NOT EXISTS audit_log (
    id          SERIAL PRIMARY KEY,
    evento      VARCHAR(100) NOT NULL,
    venta_id    VARCHAR(100),
    producto_id INTEGER,
    sucursal    VARCHAR(50),
    cliente     VARCHAR(100),
    cantidad    INTEGER,
    total       NUMERIC(10,2),
    payload     JSONB NOT NULL,
    recibido_en TIMESTAMP DEFAULT NOW()
);

-- Índices para búsquedas rápidas
CREATE INDEX IF NOT EXISTS idx_audit_evento     ON audit_log(evento);
CREATE INDEX IF NOT EXISTS idx_audit_venta_id   ON audit_log(venta_id);
CREATE INDEX IF NOT EXISTS idx_audit_recibido   ON audit_log(recibido_en DESC);
