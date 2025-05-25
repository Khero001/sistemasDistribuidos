-- SCHEMA DE BASE DE DATOS PARA INVENTARIO Y LOGÍSTICA EN CASSANDRA

-- 1. CREACIÓN DEL KEYSPACE PRINCIPAL
-- Define el Keyspace 'inventario_logistica' con una estrategia de replicación de topología de red.
-- 'datacenter1': 3 significa que cada pieza de datos tendrá 3 copias en 'datacenter1'.
-- Si tu clúster tiene más de un centro de datos lógico (definido en cassandra-rackdc.properties),
-- puedes añadir más entradas aquí (ej. 'datacenter2': 3).
CREATE KEYSPACE IF NOT EXISTS inventario_logistica
WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 4};

---

-- 2. SELECCIÓN DEL KEYSPACE
-- Una vez que el keyspace es creado o verificado, lo seleccionamos para que las tablas
-- se creen dentro de él.
USE inventario_logistica;

---

-- 3. TABLA: articulos_por_sucursal
-- Propósito: Almacena el inventario de artículos por cada sucursal.
-- Clave de Partición: sucursal_id (agrupa datos por sucursal)
-- Clave de Agrupación: articulo_id (ordena artículos dentro de la sucursal)
CREATE TABLE IF NOT EXISTS articulos_por_sucursal (
    sucursal_id text,
    articulo_id text,
    nombre_articulo text,
    descripcion text,
    cantidad int,
    capacidad_espacio int,
    capacidad_max_sucursal int,
    timestamp_ultima_actualizacion timestamp,
    PRIMARY KEY (sucursal_id, articulo_id)
);

---

-- 4. TABLA: clientes
-- Propósito: Almacena la información de los clientes.
-- Clave Primaria: cliente_id (UUID garantiza buena distribución y acceso directo)
CREATE TABLE IF NOT EXISTS clientes (
    cliente_id uuid PRIMARY KEY,
    nombre text,
    apellido text,
    direccion text,
    telefono text,
    email text,
    fecha_registro timestamp
);

---

-- 5. TABLA: guias_envio_por_id
-- Propósito: Registra cada transacción de venta o envío, consultable por el ID de la guía.
-- Clave Primaria: guia_id (UUID permite acceso directo y distribuye bien)
CREATE TABLE IF NOT EXISTS guias_envio_por_id (
    guia_id uuid PRIMARY KEY,
    articulo_id text,
    nombre_articulo text,
    cantidad_vendida int,
    sucursal_origen_id text,
    cliente_id uuid,
    fecha_venta timestamp,
    estado_envio text
);

---

-- 6. TABLA: guias_envio_por_sucursal_fecha
-- Propósito: Tabla auxiliar para consultas de guías de envío por sucursal y rango de fechas.
-- Clave de Partición: (sucursal_origen_id, fecha_venta) (agrupa por sucursal y día)
-- Clave de Agrupación: guia_id (ordena guías dentro de la partición)
-- Orden de agrupación: Por fecha de venta descendente y luego por guia_id descendente.
CREATE TABLE IF NOT EXISTS guias_envio_por_sucursal_fecha (
    sucursal_origen_id text,
    fecha_venta date,
    guia_id uuid,
    articulo_id text,
    nombre_articulo text,
    cantidad_vendida int,
    cliente_id uuid,
    estado_envio text,
    PRIMARY KEY (sucursal_origen_id, fecha_venta, guia_id)
) WITH CLUSTERING ORDER BY (fecha_venta DESC, guia_id DESC);
