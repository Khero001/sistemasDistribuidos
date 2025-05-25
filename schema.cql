-- Eliminar el Keyspace si existe para asegurar un despliegue limpio
-- ¡ADVERTENCIA! Esto borrará permanentemente todos los datos en este Keyspace.
DROP KEYSPACE IF EXISTS inventario_logistica;

-- Crear el Keyspace
-- replication_factor = 3 con SimpleStrategy es una configuración común para clústeres
-- de un solo centro de datos, permitiendo tolerar la falla de hasta 1 nodo.
CREATE KEYSPACE IF NOT EXISTS inventario_logistica
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

-- Usar el Keyspace
USE inventario_logistica;

---

-- Tabla de Sucursales
-- Almacena información maestra de cada sucursal.
-- sucursal_id: Identificador único de la sucursal (UUID)
-- nombre_sucursal: Nombre descriptivo de la sucursal
-- direccion_ip: Dirección IP de la sucursal
-- ciudad: Ciudad donde se encuentra la sucursal
CREATE TABLE IF NOT EXISTS sucursales (
    sucursal_id uuid PRIMARY KEY, -- CAMBIADO A UUID
    nombre_sucursal text,
    direccion_ip text,
    ciudad text
);

---

-- Tabla de Artículos por Sucursal
-- Almacena el inventario de artículos en cada sucursal.
-- sucursal_id: ID de la sucursal (UUID)
-- articulo_id: ID único del artículo (UUID)
-- nombre: Nombre del artículo
-- descripcion: Descripción detallada del artículo
-- cantidad: Cantidad actual del artículo en stock
-- unidad_medida: Unidad de medida del artículo (ej. "unidades", "kg", "litros")
-- capacidad_almacenamiento: Capacidad de almacenamiento requerida por unidad del artículo (ej. m3, litros)
CREATE TABLE IF NOT EXISTS articulos_por_sucursal (
    sucursal_id uuid, -- CONSISTENTE CON UUID
    articulo_id uuid, -- CONSISTENTE CON UUID
    nombre text,
    descripcion text,
    cantidad int,
    unidad_medida text,
    capacidad_almacenamiento float,
    PRIMARY KEY ((sucursal_id), articulo_id)
);

---

-- Tabla de Clientes
-- Almacena la información de los clientes.
-- cliente_id: ID único del cliente (UUID)
-- nombre: Nombre del cliente
-- apellido: Apellido del cliente
-- direccion: Dirección del cliente
-- telefono: Número de teléfono del cliente
-- email: Correo electrónico del cliente
CREATE TABLE IF NOT EXISTS clientes (
    cliente_id uuid PRIMARY KEY, -- CONSISTENTE CON UUID
    nombre text,
    apellido text,
    direccion text,
    telefono text,
    email text
);

---

-- Tabla de Guías de Envío por ID
-- Consulta principal por ID de guía de envío.
-- guia_id: ID único de la guía de envío (UUID)
-- cliente_id: ID del cliente asociado al envío (UUID)
-- sucursal_origen_id: ID de la sucursal de origen del envío (UUID)
-- sucursal_destino_id: ID de la sucursal de destino del envío (UUID)
-- fecha_venta: Fecha de creación de la guía (ej. fecha del pedido)
-- hora_venta: Hora de creación de la guía
-- estado_envio: Estado actual del envío (ej. 'PENDIENTE', 'EN_TRANSITO', 'ENTREGADO')
-- peso_kg: Peso total del envío en kilogramos
-- volumen_m3: Volumen total del envío en metros cúbicos
-- valor_declarado: Valor monetario declarado del envío
-- direccion_destino: Dirección completa de destino
-- coordenadas_destino: Coordenadas geográficas del destino
-- articulos_enviados: Mapa de nombres de artículos y sus cantidades
CREATE TABLE IF NOT EXISTS guias_envio_por_id (
    guia_id uuid PRIMARY KEY, -- CONSISTENTE CON UUID
    cliente_id uuid,          -- CONSISTENTE CON UUID
    sucursal_origen_id uuid,  -- CONSISTENTE CON UUID
    sucursal_destino_id uuid, -- CONSISTENTE CON UUID
    fecha_venta date,
    hora_venta time,
    estado_envio text,
    peso_kg float,
    volumen_m3 float,
    valor_declarado float,
    direccion_destino text,
    coordenadas_destino text,
    articulos_enviados map<text, int>
);

---

-- Tabla de Guías de Envío por Sucursal y Fecha
-- Consulta optimizada para buscar guías desde una sucursal en una fecha específica.
-- sucursal_origen_id: ID de la sucursal de origen (UUID)
-- fecha_venta: Fecha de creación de la guía (parte de la clave de agrupación)
-- guia_id: ID de la guía (parte de la clave de agrupación, permite unicidad y orden)
-- cliente_id: ID del cliente asociado al envío (UUID)
-- sucursal_destino_id: ID de la sucursal de destino del envío (UUID)
-- hora_venta: Hora de creación de la guía
-- estado_envio: Estado actual del envío
-- peso_kg: Peso total del envío
-- volumen_m3: Volumen total del envío
-- valor_declarado: Valor monetario declarado
-- direccion_destino: Dirección completa de destino
-- coordenadas_destino: Coordenadas geográficas del destino
-- articulos_enviados: Mapa de nombres de artículos y sus cantidades
CREATE TABLE IF NOT EXISTS guias_envio_por_sucursal_fecha (
    sucursal_origen_id uuid, -- CONSISTENTE CON UUID
    fecha_venta date,
    guia_id uuid,             -- CONSISTENTE CON UUID
    cliente_id uuid,          -- CONSISTENTE CON UUID
    sucursal_destino_id uuid, -- CONSISTENTE CON UUID
    hora_venta time,
    estado_envio text,
    peso_kg float,
    volumen_m3 float,
    valor_declarado float,
    direccion_destino text,
    coordenadas_destino text,
    articulos_enviados map<text, int>,
    PRIMARY KEY ((sucursal_origen_id, fecha_venta), guia_id)
);
