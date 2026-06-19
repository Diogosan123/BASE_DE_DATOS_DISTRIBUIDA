-- Tabla de productos con stock
CREATE TABLE IF NOT EXISTS productos (
    id          SERIAL PRIMARY KEY,
    nombre      VARCHAR(100) NOT NULL,
    stock       INTEGER NOT NULL CHECK (stock >= 0),
    precio      NUMERIC(10,2) NOT NULL,
    sucursal    VARCHAR(50) NOT NULL
);

-- Datos iniciales: productos de farmacia
INSERT INTO productos (nombre, stock, precio, sucursal) VALUES
    ('Paracetamol 500mg',  100, 0.50,  'Guayaquil-Norte'),
    ('Ibuprofeno 400mg',    80, 0.75,  'Guayaquil-Norte'),
    ('Amoxicilina 500mg',   50, 1.20,  'Guayaquil-Sur'),
    ('Omeprazol 20mg',      60, 0.90,  'Quito-Centro'),
    ('Metformina 850mg',    40, 0.60,  'Quito-Centro');

-- Tabla de log de movimientos de inventario (outbox pattern)
CREATE TABLE IF NOT EXISTS inventory_outbox (
    id          SERIAL PRIMARY KEY,
    evento      VARCHAR(50) NOT NULL,
    payload     JSONB NOT NULL,
    procesado   BOOLEAN DEFAULT FALSE,
    creado_en   TIMESTAMP DEFAULT NOW()
);
