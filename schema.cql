-- Elimina el keyspace si existe, para un reinicio limpio
DROP KEYSPACE IF EXISTS inventario_logistica;

-- Crea el keyspace
CREATE KEYSPACE IF NOT EXISTS inventario_logistica WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE inventario_logistica;

-- Tabla para almacenar información sobre las sucursales
CREATE TABLE IF NOT EXISTS sucursales (
    sucursal_id UUID PRIMARY KEY,
    nombre_sucursal TEXT,
    direccion_ip TEXT,
    ciudad TEXT
);

CREATE INDEX IF NOT EXISTS ON sucursales (direccion_ip);

-- Tabla para almacenar artículos en cada sucursal
-- La clave primaria compuesta (sucursal_id, articulo_id) permite consultar artículos específicos de una sucursal.
-- sucursal_id es la clave de partición, distribuyendo los datos por sucursal.
CREATE TABLE IF NOT EXISTS articulos_por_sucursal (
    sucursal_id UUID,
    articulo_id UUID,
    nombre TEXT,
    descripcion TEXT,
    cantidad INT,
    unidad_medida TEXT,
    capacidad_almacenamiento FLOAT, -- Espacio que ocupa una unidad de este artículo (ej. m3)
    PRIMARY KEY ((sucursal_id), articulo_id)
);

-- Tabla para almacenar información sobre los clientes
CREATE TABLE IF NOT EXISTS clientes (
    cliente_id UUID PRIMARY KEY,
    nombre TEXT,
    apellido TEXT,
    direccion TEXT,
    telefono TEXT,
    email TEXT
);

-- Tabla para almacenar guías de envío, consultables por el ID de la guía
-- Contiene todos los detalles de la guía, incluyendo un mapa de artículos enviados.
CREATE TABLE IF NOT EXISTS guias_envio_por_id (
    guia_id UUID PRIMARY KEY,
    cliente_id UUID,
    sucursal_origen_id UUID,
    sucursal_destino_id UUID,
    fecha_venta DATE,
    hora_venta TIME,
    estado_envio TEXT,
    peso_kg FLOAT,
    volumen_m3 FLOAT,
    valor_declarado DECIMAL,
    direccion_destino TEXT,
    coordenadas_destino TEXT,
    articulos_enviados MAP<TEXT, INT> -- Mapa de nombre_articulo -> cantidad
);

-- Materialized view o tabla duplicada para consultar guías de envío por sucursal y fecha
-- La clave de partición es (sucursal_origen_id, fecha_venta) para búsquedas eficientes.
-- guia_id es la clave de agrupación para ordenar las guías dentro de una partición.
CREATE TABLE IF NOT EXISTS guias_envio_por_sucursal_fecha (
    sucursal_origen_id UUID,
    fecha_venta DATE,
    guia_id UUID,
    cliente_id UUID,
    sucursal_destino_id UUID,
    hora_venta TIME,
    estado_envio TEXT,
    peso_kg FLOAT,
    volumen_m3 FLOAT,
    valor_declarado DECIMAL,
    direccion_destino TEXT,
    coordenadas_destino TEXT,
    articulos_enviados MAP<TEXT, INT>, -- Mapa de nombre_articulo -> cantidad
    PRIMARY KEY ((sucursal_origen_id, fecha_venta), guia_id)
) WITH CLUSTERING ORDER BY (guia_id ASC); -- ¡CORRECCIÓN AQUÍ! Solo guia_id puede estar en CLUSTERING ORDER BY
