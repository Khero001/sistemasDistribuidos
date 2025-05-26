-- DATOS DE PRUEBA AMPLIADOS PARA EL ESQUEMA DE INVENTARIO Y LOGÍSTICA EN CASSANDRA

-- Selecciona el Keyspace
USE inventario_logistica;

---
-- 1. Insertar datos en 'sucursales'
INSERT INTO sucursales (sucursal_id, nombre_sucursal, direccion_ip, ciudad) VALUES (d1a1b1c1-1001-4111-8111-000000000001, 'Sucursal Alpha', '192.168.1.101', 'Ciudad A');
INSERT INTO sucursales (sucursal_id, nombre_sucursal, direccion_ip, ciudad) VALUES (d1a1b1c1-1001-4111-8111-000000000002, 'Sucursal Beta', '192.168.1.102', 'Ciudad B');
INSERT INTO sucursales (sucursal_id, nombre_sucursal, direccion_ip, ciudad) VALUES (d1a1b1c1-1001-4111-8111-000000000003, 'Sucursal Gamma', '192.168.1.96', 'Ciudad C');
INSERT INTO sucursales (sucursal_id, nombre_sucursal, direccion_ip, ciudad) VALUES (d1a1b1c1-1001-4111-8111-000000000004, 'Sucursal Delta', '192.168.1.97', 'Ciudad D');

---
-- 2. Insertar datos en 'articulos_por_sucursal'
-- Sucursal Alpha
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES (d1a1b1c1-1001-4111-8111-000000000001, 1a1a1a1a-1111-4111-8111-000000000001, 'Portatil Gamer X1', 'Portátil de alto rendimiento para juegos', 50, 'unidades', 0.05);

INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES (d1a1b1c1-1001-4111-8111-000000000001, 1a1a1a1a-1111-4111-8111-000000000002, 'Monitor Curvo 27"', 'Monitor con gran inmersión visual', 120, 'unidades', 0.03);

INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES (d1a1b1c1-1001-4111-8111-000000000001, 1a1a1a1a-1111-4111-8111-000000000003, 'Teclado Mecánico RGB', 'Teclado con switches mecánicos y retroiluminación RGB', 80, 'unidades', 0.005);

-- Sucursal Beta
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES (d1a1b1c1-1001-4111-8111-000000000002, 1a1a1a1a-1111-4111-8111-000000000011, 'Smartwatch Modelo A', 'Reloj inteligente con funciones de salud', 150, 'unidades', 0.001);

INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES (d1a1b1c1-1001-4111-8111-000000000002, 1a1a1a1a-1111-4111-8111-000000000012, 'Auriculares Inalámbricos', 'Auriculares de alta fidelidad con cancelación de ruido', 200, 'unidades', 0.0005);

-- Sucursal Gamma
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES (d1a1b1c1-1001-4111-8111-000000000003, 1a1a1a1a-1111-4111-8111-000000000021, 'Drone Profesional', 'Drone con cámara 4K y GPS', 20, 'unidades', 0.1);

INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES (d1a1b1c1-1001-4111-8111-000000000003, 1a1a1a1a-1111-4111-8111-000000000022, 'Cámara Mirrorless', 'Cámara de fotos avanzada con lentes intercambiables', 30, 'unidades', 0.02);

-- Sucursal Delta
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES (d1a1b1c1-1001-4111-8111-000000000004, 1a1a1a1a-1111-4111-8111-000000000031, 'Consola de Videojuegos', 'Consola de última generación', 70, 'unidades', 0.08);

INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES (d1a1b1c1-1001-4111-8111-000000000004, 1a1a1a1a-1111-4111-8111-000000000032, 'Juego VR', 'Experiencia de realidad virtual inmersiva', 100, 'unidades', 0.01);


---
-- 3. Insertar datos en 'clientes'
INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES (c1b2c3d4-e5f6-7890-1234-567890abcd01, 'Ana', 'Torres', 'Av. Siempre Viva 742, Ciudad A', '5511223344', 'ana.torres@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES (c1b2c3d4-e5f6-7890-1234-567890abcd02, 'Luis', 'Gomez', 'Calle Falsa 123, Ciudad B', '5555667788', 'luis.gomez@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES (c1b2c3d4-e5f6-7890-1234-567890abcd03, 'Marta', 'Ramirez', 'Blvd. de los Sueños 45, Ciudad C', '5599001122', 'marta.ramirez@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES (c1b2c3d4-e5f6-7890-1234-567890abcd04, 'Carlos', 'Diaz', 'Paseo del Sol 8, Ciudad D', '5533445566', 'carlos.diaz@email.com');

---
-- 4. Insertar datos en 'guias_envio_por_id' y 'guias_envio_por_sucursal_fecha'
-- Venta 1: Sucursal Alpha -> Cliente Ana Torres, Portatil Gamer X1
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES (11111111-1111-4111-8111-111111111111, c1b2c3d4-e5f6-7890-1234-567890abcd01, d1a1b1c1-1001-4111-8111-000000000001, d1a1b1c1-1001-4111-8111-000000000004, '2025-05-20', '10:00:00', 'EN_TRANSITO', 2.5, 0.05, 1200.00, 'Av. Siempre Viva 742, Ciudad D', '19.4326,-99.1332', {'Portatil Gamer X1': 1, 'Mouse Gamer': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES (d1a1b1c1-1001-4111-8111-000000000001, '2025-05-20', 11111111-1111-4111-8111-111111111111, c1b2c3d4-e5f6-7890-1234-567890abcd01, d1a1b1c1-1001-4111-8111-000000000004, '10:00:00', 'EN_TRANSITO', 2.5, 0.05, 1200.00, 'Av. Siempre Viva 742, Ciudad D', '19.4326,-99.1332', {'Portatil Gamer X1': 1, 'Mouse Gamer': 1});

-- Venta 2: Sucursal Beta -> Cliente Luis Gomez, Smartwatch y Auriculares
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES (22222222-2222-4222-8222-222222222222, c1b2c3d4-e5f6-7890-1234-567890abcd02, d1a1b1c1-1001-4111-8111-000000000002, d1a1b1c1-1001-4111-8111-000000000001, '2025-05-21', '14:30:00', 'PENDIENTE', 0.3, 0.002, 350.00, 'Calle Falsa 123, Ciudad A', '19.4326,-99.1332', {'Smartwatch Modelo A': 1, 'Auriculares Inalámbricos': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES (d1a1b1c1-1001-4111-8111-000000000002, '2025-05-21', 22222222-2222-4222-8222-222222222222, c1b2c3d4-e5f6-7890-1234-567890abcd02, d1a1b1c1-1001-4111-8111-000000000001, '14:30:00', 'PENDIENTE', 0.3, 0.002, 350.00, 'Calle Falsa 123, Ciudad A', '19.4326,-99.1332', {'Smartwatch Modelo A': 1, 'Auriculares Inalámbricos': 1});

-- Venta 3: Sucursal Gamma -> Cliente Marta Ramirez, Drone Profesional
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES (33333333-3333-4333-8333-333333333333, c1b2c3d4-e5f6-7890-1234-567890abcd03, d1a1b1c1-1001-4111-8111-000000000003, d1a1b1c1-1001-4111-8111-000000000002, '2025-05-22', '09:15:00', 'ENTREGADO', 1.8, 0.1, 800.00, 'Blvd. de los Sueños 45, Ciudad B', '19.4326,-99.1332', {'Drone Profesional': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES (d1a1b1c1-1001-4111-8111-000000000003, '2025-05-22', 33333333-3333-4333-8333-333333333333, c1b2c3d4-e5f6-7890-1234-567890abcd03, d1a1b1c1-1001-4111-8111-000000000002, '09:15:00', 'ENTREGADO', 1.8, 0.1, 800.00, 'Blvd. de los Sueños 45, Ciudad B', '19.4326,-99.1332', {'Drone Profesional': 1});

-- Venta 4: Sucursal Delta -> Cliente Carlos Diaz, Consola de Videojuegos
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES (44444444-4444-4444-8444-444444444444, c1b2c3d4-e5f6-7890-1234-567890abcd04, d1a1b1c1-1001-4111-8111-000000000004, d1a1b1c1-1001-4111-8111-000000000003, '2025-05-23', '11:00:00', 'EN_TRANSITO', 4.0, 0.08, 500.00, 'Paseo del Sol 8, Ciudad C', '19.4326,-99.1332', {'Consola de Videojuegos': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES (d1a1b1c1-1001-4111-8111-000000000004, '2025-05-23', 44444444-4444-4444-8444-444444444444, c1b2c3d4-e5f6-7890-1234-567890abcd04, d1a1b1c1-1001-4111-8111-000000000003, '11:00:00', 'EN_TRANSITO', 4.0, 0.08, 500.00, 'Paseo del Sol 8, Ciudad C', '19.4326,-99.1332', {'Consola de Videojuegos': 1});

-- Movimiento entre sucursales: Sucursal Alpha -> Sucursal Beta, Monitor Curvo 27"
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES (55555555-5555-4555-8555-555555555555, null, d1a1b1c1-1001-4111-8111-000000000001, d1a1b1c1-1001-4111-8111-000000000002, '2025-05-24', '16:00:00', 'EN_TRANSITO', 3.0, 0.03, 0.00, 'Sucursal Beta', 'Coord Beta', {'Monitor Curvo 27"': 5});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES (d1a1b1c1-1001-4111-8111-000000000001, '2025-05-24', 55555555-5555-4555-8555-555555555555, null, d1a1b1c1-1001-4111-8111-000000000002, '16:00:00', 'EN_TRANSITO', 3.0, 0.03, 0.00, 'Sucursal Beta', 'Coord Beta', {'Monitor Curvo 27"': 5});
