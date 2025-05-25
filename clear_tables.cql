-- Script para vaciar todas las tablas del keyspace inventario_logistica

USE inventario_logistica;

TRUNCATE articulos_por_sucursal;
TRUNCATE clientes;
TRUNCATE guias_envio_por_id;
TRUNCATE guias_envio_por_sucursal_fecha;

