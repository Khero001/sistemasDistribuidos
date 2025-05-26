-- DATOS DE PRUEBA AMPLIADOS PARA EL ESQUEMA DE INVENTARIO Y LOGÍSTICA EN CASSANDRA

-- Selecciona el Keyspace
USE inventario_logistica;

---

-- Definición de UUIDs para las sucursales para asegurar consistencia
-- Estos UUIDs se usarán a lo largo del archivo.
-- S001 (Alpha): d1a1b1c1-1001-4111-8111-000000000001
-- S002 (Beta): d1a1b1c1-1001-4111-8111-000000000002
-- S003 (Gamma): d1a1b1c1-1001-4111-8111-000000000003
-- S004 (Delta): d1a1b1c1-1001-4111-8111-000000000004

---

-- 1. Insertar datos en 'sucursales'
-- 4 sucursales con nombres, direcciones IP y ciudades específicas, usando UUIDs para sucursal_id

INSERT INTO sucursales (sucursal_id, nombre_sucursal, direccion_ip, ciudad)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, 'Alpha', '192.168.1.101', 'Ciudad de México');

INSERT INTO sucursales (sucursal_id, nombre_sucursal, direccion_ip, ciudad)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, 'Beta', '192.168.1.102', 'Guadalajara');

INSERT INTO sucursales (sucursal_id, nombre_sucursal, direccion_ip, ciudad)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, 'Gamma', '192.168.1.96', 'Monterrey');

INSERT INTO sucursales (sucursal_id, nombre_sucursal, direccion_ip, ciudad)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, 'Delta', '192.168.1.97', 'Puebla');

---

-- 2. Insertar datos en 'articulos_por_sucursal'
-- 40 registros de artículos distribuidos aleatoriamente entre las 4 sucursales.
-- Se incluyen 'unidad_medida' y 'capacidad_almacenamiento' por artículo, y todos los IDs son UUIDs.

-- Artículos para Sucursal Alpha (S001)
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)1a1a1a1a-1111-4111-8111-000000000001, 'Laptop Gamer Pro', 'Portátil de alta gama para juegos', 25, 'unidades', 2.5);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)1a1a1a1a-1111-4111-8111-000000000002, 'Monitor UltraWide 34"', 'Monitor curvo de 34 pulgadas', 15, 'unidades', 1.8);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)1a1a1a1a-1111-4111-8111-000000000003, 'Teclado Mecánico RGB', 'Teclado con switches Cherry MX', 40, 'unidades', 0.6);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)1a1a1a1a-1111-4111-8111-000000000004, 'Mouse Inalámbrico Pro', 'Mouse ergonómico de alta precisión', 50, 'unidades', 0.3);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)1a1a1a1a-1111-4111-8111-000000000005, 'Auriculares Noise Cancelling', 'Auriculares con cancelación activa de ruido', 30, 'unidades', 0.9);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)1a1a1a1a-1111-4111-8111-000000000006, 'Disco SSD 2TB NVMe', 'Unidad de estado sólido ultra rápida', 20, 'unidades', 0.1);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)1a1a1a1a-1111-4111-8111-000000000007, 'Webcam Full HD', 'Cámara web para streaming y videollamadas', 35, 'unidades', 0.2);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)1a1a1a1a-1111-4111-8111-000000000008, 'Impresora Multifuncional', 'Impresora láser con escáner y copiadora', 10, 'unidades', 3.0);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)1a1a1a1a-1111-4111-8111-000000000009, 'Router Wi-Fi 6', 'Enrutador de alta velocidad', 20, 'unidades', 0.7);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)1a1a1a1a-1111-4111-8111-000000000010, 'Tableta Gráfica Profesional', 'Tableta para diseño gráfico', 12, 'unidades', 1.0);

-- Artículos para Sucursal Beta (S002)
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)1a1a1a1a-1111-4111-8111-000000000011, 'Smartphone Premium', 'Teléfono inteligente de última generación', 45, 'unidades', 0.5);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)1a1a1a1a-1111-4111-8111-000000000012, 'Smartwatch Deportivo', 'Reloj inteligente con funciones deportivas', 30, 'unidades', 0.2);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)1a1a1a1a-1111-4111-8111-000000000013, 'Altavoz Bluetooth Potente', 'Altavoz portátil con gran calidad de sonido', 28, 'unidades', 0.8);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)1a1a1a1a-1111-4111-8111-000000000014, 'Cámara de Seguridad IP', 'Cámara de vigilancia para interiores', 22, 'unidades', 0.4);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)1a1a1a1a-1111-4111-8111-000000000015, 'Drone con Cámara 4K', 'Drone para fotografía y video aéreo', 8, 'unidades', 2.0);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)1a1a1a1a-1111-4111-8111-000000000016, 'Consola de Videojuegos', 'Consola de última generación', 18, 'unidades', 1.5);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)1a1a1a1a-1111-4111-8111-000000000017, 'Juego de Realidad Virtual', 'Kit completo de VR', 7, 'unidades', 2.2);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)1a1a1a1a-1111-4111-8111-000000000018, 'Proyector Portátil', 'Proyector compacto para cine en casa', 10, 'unidades', 1.3);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)1a1a1a1a-1111-4111-8111-000000000019, 'Estabilizador de Smartphone', 'Gimbal para videos con teléfono', 20, 'unidades', 0.7);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)1a1a1a1a-1111-4111-8111-000000000020, 'Tarjeta Gráfica RTX 4090', 'Tarjeta de video de gama alta', 5, 'unidades', 1.2);

-- Artículos para Sucursal Gamma (S003)
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)1a1a1a1a-1111-4111-8111-000000000021, 'Microondas Digital', 'Horno microondas con funciones inteligentes', 15, 'unidades', 2.5);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)1a1a1a1a-1111-4111-8111-000000000022, 'Cafetera Programable', 'Cafetera con temporizador y molinillo', 20, 'unidades', 1.2);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)1a1a1a1a-1111-4111-8111-000000000023, 'Aspiradora Robot Inteligente', 'Aspiradora autónoma con mapeo láser', 10, 'unidades', 1.8);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)1a1a1a1a-1111-4111-8111-000000000024, 'Refrigerador Inverter', 'Refrigerador de bajo consumo energético', 5, 'unidades', 15.0);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)1a1a1a1a-1111-4111-8111-000000000025, 'Lavadora Carga Frontal', 'Lavadora de alta eficiencia', 7, 'unidades', 10.0);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)1a1a1a1a-1111-4111-8111-000000000026, 'Secadora de Ropa Eléctrica', 'Secadora con sensor de humedad', 6, 'unidades', 9.0);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)1a1a1a1a-1111-4111-8111-000000000027, 'Estufa de Inducción', 'Estufa moderna y eficiente', 8, 'unidades', 4.0);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)1a1a1a1a-1111-4111-8111-000000000028, 'Licuadora de Alta Potencia', 'Licuadora para smoothies y jugos', 25, 'unidades', 0.8);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)1a1a1a1a-1111-4111-8111-000000000029, 'Horno Eléctrico de Convección', 'Horno para cocinar y hornear', 12, 'unidades', 3.5);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)1a1a1a1a-1111-4111-8111-000000000030, 'Purificador de Aire Inteligente', 'Sistema de purificación de aire', 18, 'unidades', 1.0);

-- Artículos para Sucursal Delta (S004)
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)1a1a1a1a-1111-4111-8111-000000000031, 'Set de Herramientas Completo', 'Kit de herramientas para el hogar', 30, 'unidades', 2.0);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)1a1a1a1a-1111-4111-8111-000000000032, 'Taladro Inalámbrico', 'Taladro percutor a batería', 20, 'unidades', 1.5);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)1a1a1a1a-1111-4111-8111-000000000033, 'Sierra Eléctrica Circular', 'Sierra para cortes precisos en madera', 10, 'unidades', 3.0);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)1a1a1a1a-1111-4111-8111-000000000034, 'Compresor de Aire Portátil', 'Compresor para inflar neumáticos y más', 8, 'unidades', 4.0);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)1a1a1a1a-1111-4111-8111-000000000035, 'Lámpara LED de Escritorio', 'Lámpara con diferentes modos de luz', 40, 'unidades', 0.5);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)1a1a1a1a-1111-4111-8111-000000000036, 'Sistema de Alarma Inteligente', 'Kit de seguridad para el hogar', 15, 'unidades', 1.0);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)1a1a1a1a-1111-4111-8111-000000000037, 'Cargador Portátil Power Bank', 'Batería externa para dispositivos móviles', 50, 'unidades', 0.3);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)1a1a1a1a-1111-4111-8111-000000000038, 'Kit de Jardinería Completo', 'Herramientas para el cuidado del jardín', 25, 'unidades', 2.5);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)1a1a1a1a-1111-4111-8111-000000000039, 'Asador de Carbón Premium', 'Asador para exteriores', 7, 'unidades', 7.0);
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)1a1a1a1a-1111-4111-8111-000000000040, 'Silla de Oficina Ergonómica', 'Silla cómoda para largas jornadas', 12, 'unidades', 5.0);

---

-- 3. Insertar datos en 'clientes'
-- 15 clientes con información de contacto

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd01, 'Ana', 'Torres', 'Av. Reforma 100, Col. Juárez, CDMX', '5511223344', 'ana.torres@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd02, 'Carlos', 'Ruiz', 'Calle del Sol 20, Col. Centro, Guadalajara', '5522334455', 'carlos.ruiz@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd03, 'Elena', 'Gómez', 'Paseo de la Luna 50, Col. San Pedro, Monterrey', '5533445566', 'elena.gomez@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd04, 'Fernando', 'Díaz', 'Blvd. Las Palmas 75, Col. Floresta, Puebla', '5544556677', 'fernando.diaz@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd05, 'Gabriela', 'Vargas', 'Plaza Mayor 15, Col. Independencia, Querétaro', '5555667788', 'gabriela.vargas@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd06, 'Héctor', 'Soto', 'Av. Juárez 30, Col. Roma, CDMX', '5566778899', 'hector.soto@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd07, 'Isabel', 'Ramos', 'Guerrero Negro 45, Col. Condesa, CDMX', '5577889900', 'isabel.ramos@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd08, 'Jorge', 'Mendoza', 'Callejon del Beso 1, Col. Jardines, Guanajuato', '5588990011', 'jorge.mendoza@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd09, 'Karen', 'Navarro', 'Boulevard del Parque 80, Col. Del Valle, San Luis Potosí', '5599001122', 'karen.navarro@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd10, 'Luis', 'Castro', 'Via Láctea 5, Col. Estrella, Mérida', '5500112233', 'luis.castro@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd11, 'Marta', 'Flores', 'Calle Tulipanes 12, Col. Florida, Cancún', '5510203040', 'marta.flores@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd12, 'Pedro', 'Salazar', 'Av. Principal 250, Col. Centro, Tijuana', '5521436587', 'pedro.salazar@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd13, 'Sofía', 'Reyes', 'Calle Río Bravo 30, Col. Americana, Guadalajara', '5532547698', 'sofia.reyes@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd14, 'Daniel', 'Campos', 'Sendero del Bosque 15, Col. Sierra, Monterrey', '5543769810', 'daniel.campos@email.com');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
VALUES ((UUID)c1b2c3d4-e5f6-7890-1234-567890abcd15, 'Valeria', 'Cruz', 'Circuito Interior 60, Col. Del Valle, CDMX', '5554981023', 'valeria.cruz@email.com');

---

-- 4. Insertar datos en 'guias_envio_por_id' y 'guias_envio_por_sucursal_fecha'
-- Simular 20 ventas y 10 movimientos entre sucursales.
-- Cada venta/movimiento debe tener su registro en ambas tablas, con todos los IDs como UUIDs.
-- Las fechas y horas se ajustan a los tipos 'date' y 'time' de Cassandra.
-- 'articulos_enviados' se usa como mapa<text, int>.

-- Ventas (total 20)
-- Venta 1 (Alpha - Laptop Gamer Pro)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)11111111-1111-4111-8111-111111111111, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd01, (UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-20', '10:00:00', 'EN_PREPARACION', 3.0, 0.015, 1500.00, 'Av. Reforma 100, Col. Juárez, CDMX', '19.4326,-99.1332', {'Laptop Gamer Pro': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-20', (UUID)11111111-1111-4111-8111-111111111111, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd01, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '10:00:00', 'EN_PREPARACION', 3.0, 0.015, 1500.00, 'Av. Reforma 100, Col. Juárez, CDMX', '19.4326,-99.1332', {'Laptop Gamer Pro': 1});

-- Venta 2 (Beta - Smartphone Premium)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)22222222-2222-4222-8222-222222222222, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd02, (UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-20', '11:00:00', 'EN_TRANSITO', 0.5, 0.001, 800.00, 'Calle del Sol 20, Col. Centro, Guadalajara', '20.6736,-103.3406', {'Smartphone Premium': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-20', (UUID)22222222-2222-4222-8222-222222222222, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd02, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '11:00:00', 'EN_TRANSITO', 0.5, 0.001, 800.00, 'Calle del Sol 20, Col. Centro, Guadalajara', '20.6736,-103.3406', {'Smartphone Premium': 1});

-- Venta 3 (Gamma - Aspiradora Robot Inteligente)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)33333333-3333-4333-8333-333333333333, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd03, (UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-21', '09:30:00', 'ENTREGADO', 4.0, 0.02, 350.00, 'Paseo de la Luna 50, Col. San Pedro, Monterrey', '25.6866,-100.3161', {'Aspiradora Robot Inteligente': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-21', (UUID)33333333-3333-4333-8333-333333333333, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd03, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '09:30:00', 'ENTREGADO', 4.0, 0.02, 350.00, 'Paseo de la Luna 50, Col. San Pedro, Monterrey', '25.6866,-100.3161', {'Aspiradora Robot Inteligente': 1});

-- Venta 4 (Delta - Taladro Inalámbrico)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)44444444-4444-4444-8444-444444444444, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd04, (UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-21', '13:45:00', 'PENDIENTE', 2.5, 0.01, 120.00, 'Blvd. Las Palmas 75, Col. Floresta, Puebla', '19.0414,-98.2063', {'Taladro Inalámbrico': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-21', (UUID)44444444-4444-4444-8444-444444444444, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd04, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '13:45:00', 'PENDIENTE', 2.5, 0.01, 120.00, 'Blvd. Las Palmas 75, Col. Floresta, Puebla', '19.0414,-98.2063', {'Taladro Inalámbrico': 1});

-- Venta 5 (Alpha - Teclado Mecánico RGB)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)55555555-5555-4555-8555-555555555555, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd05, (UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-22', '10:15:00', 'EN_PREPARACION', 1.0, 0.003, 80.00, 'Plaza Mayor 15, Col. Independencia, Querétaro', '20.5912,-100.3878', {'Teclado Mecánico RGB': 2});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-22', (UUID)55555555-5555-4555-8555-555555555555, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd05, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '10:15:00', 'EN_PREPARACION', 1.0, 0.003, 80.00, 'Plaza Mayor 15, Col. Independencia, Querétaro', '20.5912,-100.3878', {'Teclado Mecánico RGB': 2});

-- Venta 6 (Beta - Consola de Videojuegos)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)66666666-6666-4666-8666-666666666666, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd06, (UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-22', '15:00:00', 'EN_TRANSITO', 4.5, 0.03, 500.00, 'Av. Juárez 30, Col. Roma, CDMX', '19.4223,-99.1678', {'Consola de Videojuegos': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-22', (UUID)66666666-6666-4666-8666-666666666666, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd06, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '15:00:00', 'EN_TRANSITO', 4.5, 0.03, 500.00, 'Av. Juárez 30, Col. Roma, CDMX', '19.4223,-99.1678', {'Consola de Videojuegos': 1});

-- Venta 7 (Gamma - Refrigerador Inverter)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)77777777-7777-4777-8777-777777777777, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd07, (UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-23', '09:00:00', 'ENTREGADO', 70.0, 0.7, 1200.00, 'Guerrero Negro 45, Col. Condesa, CDMX', '19.4167,-99.1727', {'Refrigerador Inverter': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-23', (UUID)77777777-7777-4777-8777-777777777777, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd07, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '09:00:00', 'ENTREGADO', 70.0, 0.7, 1200.00, 'Guerrero Negro 45, Col. Condesa, CDMX', '19.4167,-99.1727', {'Refrigerador Inverter': 1});

-- Venta 8 (Delta - Silla de Oficina Ergonómica)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)88888888-8888-4888-8888-888888888888, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd08, (UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-23', '11:30:00', 'PENDIENTE', 15.0, 0.15, 250.00, 'Callejon del Beso 1, Col. Jardines, Guanajuato', '21.0186,-101.2588', {'Silla de Oficina Ergonómica': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-23', (UUID)88888888-8888-4888-8888-888888888888, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd08, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '11:30:00', 'PENDIENTE', 15.0, 0.15, 250.00, 'Callejon del Beso 1, Col. Jardines, Guanajuato', '21.0186,-101.2588', {'Silla de Oficina Ergonómica': 1});

-- Venta 9 (Alpha - Monitor UltraWide 34")
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)99999999-9999-4999-8999-999999999999, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd09, (UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-24', '14:00:00', 'EN_PREPARACION', 5.0, 0.03, 400.00, 'Boulevard del Parque 80, Col. Del Valle, San Luis Potosí', '22.1581,-100.9996', {'Monitor UltraWide 34"': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-24', (UUID)99999999-9999-4999-8999-999999999999, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd09, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '14:00:00', 'EN_PREPARACION', 5.0, 0.03, 400.00, 'Boulevard del Parque 80, Col. Del Valle, San Luis Potosí', '22.1581,-100.9996', {'Monitor UltraWide 34"': 1});

-- Venta 10 (Beta - Altavoz Bluetooth Potente)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)0a0a0a0a-0a0a-4a0a-8a0a-0a0a0a0a0a0a, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd10, (UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-24', '16:30:00', 'EN_TRANSITO', 1.5, 0.005, 150.00, 'Via Láctea 5, Col. Estrella, Mérida', '20.9674,-89.6242', {'Altavoz Bluetooth Potente': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-24', (UUID)0a0a0a0a-0a0a-4a0a-8a0a-0a0a0a0a0a0a, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd10, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '16:30:00', 'EN_TRANSITO', 1.5, 0.005, 150.00, 'Via Láctea 5, Col. Estrella, Mérida', '20.9674,-89.6242', {'Altavoz Bluetooth Potente': 1});

-- Venta 11 (Gamma - Licuadora de Alta Potencia)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)0b0b0b0b-0b0b-4b0b-8b0b-0b0b0b0b0b0b, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd11, (UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-25', '10:00:00', 'ENTREGADO', 2.0, 0.008, 100.00, 'Calle Tulipanes 12, Col. Florida, Cancún', '21.1619,-86.8527', {'Licuadora de Alta Potencia': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-25', (UUID)0b0b0b0b-0b0b-4b0b-8b0b-0b0b0b0b0b0b, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd11, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '10:00:00', 'ENTREGADO', 2.0, 0.008, 100.00, 'Calle Tulipanes 12, Col. Florida, Cancún', '21.1619,-86.8527', {'Licuadora de Alta Potencia': 1});

-- Venta 12 (Delta - Cargador Portátil Power Bank)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)0c0c0c0c-0c0c-4c0c-8c0c-0c0c0c0c0c0c, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd12, (UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-25', '12:00:00', 'PENDIENTE', 0.2, 0.0005, 30.00, 'Av. Principal 250, Col. Centro, Tijuana', '32.5149,-117.0382', {'Cargador Portátil Power Bank': 2});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-25', (UUID)0c0c0c0c-0c0c-4c0c-8c0c-0c0c0c0c0c0c, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd12, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '12:00:00', 'PENDIENTE', 0.2, 0.0005, 30.00, 'Av. Principal 250, Col. Centro, Tijuana', '32.5149,-117.0382', {'Cargador Portátil Power Bank': 2});

-- Venta 13 (Alpha - Auriculares Noise Cancelling)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)0d0d0d0d-0d0d-4d0d-8d0d-0d0d0d0d0d0d, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd13, (UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-26', '09:00:00', 'EN_PREPARACION', 0.8, 0.002, 180.00, 'Calle Río Bravo 30, Col. Americana, Guadalajara', '20.6736,-103.3406', {'Auriculares Noise Cancelling': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-26', (UUID)0d0d0d0d-0d0d-4d0d-8d0d-0d0d0d0d0d0d, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd13, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '09:00:00', 'EN_PREPARACION', 0.8, 0.002, 180.00, 'Calle Río Bravo 30, Col. Americana, Guadalajara', '20.6736,-103.3406', {'Auriculares Noise Cancelling': 1});

-- Venta 14 (Beta - Smartwatch Deportivo)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)0e0e0e0e-0e0e-4e0e-8e0e-0e0e0e0e0e0e, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd14, (UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-26', '11:00:00', 'EN_TRANSITO', 0.3, 0.0008, 220.00, 'Sendero del Bosque 15, Col. Sierra, Monterrey', '25.6866,-100.3161', {'Smartwatch Deportivo': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-26', (UUID)0e0e0e0e-0e0e-4e0e-8e0e-0e0e0e0e0e0e, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd14, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '11:00:00', 'EN_TRANSITO', 0.3, 0.0008, 220.00, 'Sendero del Bosque 15, Col. Sierra, Monterrey', '25.6866,-100.3161', {'Smartwatch Deportivo': 1});

-- Venta 15 (Gamma - Cafetera Programable)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd15, (UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-27', '09:30:00', 'ENTREGADO', 3.0, 0.01, 90.00, 'Circuito Interior 60, Col. Del Valle, CDMX', '19.4167,-99.1727', {'Cafetera Programable': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-27', (UUID)0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd15, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '09:30:00', 'ENTREGADO', 3.0, 0.01, 90.00, 'Circuito Interior 60, Col. Del Valle, CDMX', '19.4167,-99.1727', {'Cafetera Programable': 1});

-- Venta 16 (Delta - Lámpara LED de Escritorio)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)10101010-1010-4010-8010-101010101010, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd01, (UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-27', '13:00:00', 'PENDIENTE', 0.5, 0.002, 25.00, 'Av. Reforma 100, Col. Juárez, CDMX', '19.4326,-99.1332', {'Lámpara LED de Escritorio': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-27', (UUID)10101010-1010-4010-8010-101010101010, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd01, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '13:00:00', 'PENDIENTE', 0.5, 0.002, 25.00, 'Av. Reforma 100, Col. Juárez, CDMX', '19.4326,-99.1332', {'Lámpara LED de Escritorio': 1});

-- Venta 17 (Alpha - Disco SSD 2TB NVMe)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)11111111-1111-4111-8111-111111111112, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd02, (UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-28', '10:00:00', 'EN_PREPARACION', 0.1, 0.0001, 150.00, 'Calle del Sol 20, Col. Centro, Guadalajara', '20.6736,-103.3406', {'Disco SSD 2TB NVMe': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-28', (UUID)11111111-1111-4111-8111-111111111112, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd02, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '10:00:00', 'EN_PREPARACION', 0.1, 0.0001, 150.00, 'Calle del Sol 20, Col. Centro, Guadalajara', '20.6736,-103.3406', {'Disco SSD 2TB NVMe': 1});

-- Venta 18 (Beta - Drone con Cámara 4K)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)22222222-2222-4222-8222-222222222223, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd03, (UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-28', '12:00:00', 'EN_TRANSITO', 1.0, 0.005, 700.00, 'Paseo de la Luna 50, Col. San Pedro, Monterrey', '25.6866,-100.3161', {'Drone con Cámara 4K': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-28', (UUID)22222222-2222-4222-8222-222222222223, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd03, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '12:00:00', 'EN_TRANSITO', 1.0, 0.005, 700.00, 'Paseo de la Luna 50, Col. San Pedro, Monterrey', '25.6866,-100.3161', {'Drone con Cámara 4K': 1});

-- Venta 19 (Gamma - Aspiradora Robot Inteligente)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)33333333-3333-4333-8333-333333333334, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd04, (UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-29', '09:00:00', 'ENTREGADO', 4.0, 0.02, 350.00, 'Blvd. Las Palmas 75, Col. Floresta, Puebla', '19.0414,-98.2063', {'Aspiradora Robot Inteligente': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-29', (UUID)33333333-3333-4333-8333-333333333334, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd04, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '09:00:00', 'ENTREGADO', 4.0, 0.02, 350.00, 'Blvd. Las Palmas 75, Col. Floresta, Puebla', '19.0414,-98.2063', {'Aspiradora Robot Inteligente': 1});

-- Venta 20 (Delta - Asador de Carbón Premium)
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)44444444-4444-4444-8444-444444444445, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd05, (UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-29', '14:00:00', 'PENDIENTE', 20.0, 0.2, 180.00, 'Plaza Mayor 15, Col. Independencia, Querétaro', '20.5912,-100.3878', {'Asador de Carbón Premium': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-29', (UUID)44444444-4444-4444-8444-444444444445, (UUID)c1b2c3d4-e5f6-7890-1234-567890abcd05, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '14:00:00', 'PENDIENTE', 20.0, 0.2, 180.00, 'Plaza Mayor 15, Col. Independencia, Querétaro', '20.5912,-100.3878', {'Asador de Carbón Premium': 1});


-- Movimientos entre Sucursales (total 10)
-- Estos movimientos simulan una transferencia entre sucursales.
-- Para fines de las guías de envío, se usa NULL para cliente_id y se especifica la sucursal de destino.

-- Movimiento 1 (Alpha -> Beta): Webcam Full HD
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)a1a1a1a1-a1a1-41a1-81a1-a1a1a1a1a1a1, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-20', '10:30:00', 'EN_TRANSITO', 1.0, 0.001, 100.00, 'Sucursal Beta - Guadalajara', '20.6736,-103.3406', {'Webcam Full HD': 5});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-20', (UUID)a1a1a1a1-a1a1-41a1-81a1-a1a1a1a1a1a1, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '10:30:00', 'EN_TRANSITO', 1.0, 0.001, 100.00, 'Sucursal Beta - Guadalajara', '20.6736,-103.3406', {'Webcam Full HD': 5});

-- Movimiento 2 (Beta -> Gamma): Estabilizador de Smartphone
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)b2b2b2b2-b2b2-42b2-82b2-b2b2b2b2b2b2, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-21', '11:00:00', 'EN_TRANSITO', 1.5, 0.003, 90.00, 'Sucursal Gamma - Monterrey', '25.6866,-100.3161', {'Estabilizador de Smartphone': 3});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-21', (UUID)b2b2b2b2-b2b2-42b2-82b2-b2b2b2b2b2b2, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '11:00:00', 'EN_TRANSITO', 1.5, 0.003, 90.00, 'Sucursal Gamma - Monterrey', '25.6866,-100.3161', {'Estabilizador de Smartphone': 3});

-- Movimiento 3 (Gamma -> Delta): Purificador de Aire Inteligente
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)c3c3c3c3-c3c3-43c3-83c3-c3c3c3c3c3c3, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-22', '09:00:00', 'EN_TRANSITO', 3.0, 0.015, 200.00, 'Sucursal Delta - Puebla', '19.0414,-98.2063', {'Purificador de Aire Inteligente': 2});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-22', (UUID)c3c3c3c3-c3c3-43c3-83c3-c3c3c3c3c3c3, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '09:00:00', 'EN_TRANSITO', 3.0, 0.015, 200.00, 'Sucursal Delta - Puebla', '19.0414,-98.2063', {'Purificador de Aire Inteligente': 2});

-- Movimiento 4 (Delta -> Alpha): Kit de Jardinería Completo
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d4d4d4d4-d4d4-44d4-84d4-d4d4d4d4d4d4, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-23', '14:00:00', 'EN_TRANSITO', 10.0, 0.05, 150.00, 'Sucursal Alpha - Ciudad de México', '19.4326,-99.1332', {'Kit de Jardinería Completo': 4});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-23', (UUID)d4d4d4d4-d4d4-44d4-84d4-d4d4d4d4d4d4, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '14:00:00', 'EN_TRANSITO', 10.0, 0.05, 150.00, 'Sucursal Alpha - Ciudad de México', '19.4326,-99.1332', {'Kit de Jardinería Completo': 4});

-- Movimiento 5 (Alpha -> Gamma): Router Wi-Fi 6
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)e5e5e5e5-e5e5-45e5-85e5-e5e5e5e5e5e5, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-24', '10:00:00', 'EN_TRANSITO', 2.1, 0.004, 60.00, 'Sucursal Gamma - Monterrey', '25.6866,-100.3161', {'Router Wi-Fi 6': 3});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-24', (UUID)e5e5e5e5-e5e5-45e5-85e5-e5e5e5e5e5e5, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000003, '10:00:00', 'EN_TRANSITO', 2.1, 0.004, 60.00, 'Sucursal Gamma - Monterrey', '25.6866,-100.3161', {'Router Wi-Fi 6': 3});

-- Movimiento 6 (Beta -> Alpha): Tarjeta Gráfica RTX 4090
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)f6f6f6f6-f6f6-46f6-86f6-f6f6f6f6f6f6, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000002, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-25', '11:00:00', 'EN_TRANSITO', 2.0, 0.008, 1000.00, 'Sucursal Alpha - Ciudad de México', '19.4326,-99.1332', {'Tarjeta Gráfica RTX 4090': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-25', (UUID)f6f6f6f6-f6f6-46f6-86f6-f6f6f6f6f6f6, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '11:00:00', 'EN_TRANSITO', 2.0, 0.008, 1000.00, 'Sucursal Alpha - Ciudad de México', '19.4326,-99.1332', {'Tarjeta Gráfica RTX 4090': 1});

-- Movimiento 7 (Gamma -> Alpha): Estufa de Inducción
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)a7a7a7a7-a7a7-47a7-87a7-a7a7a7a7a7a7, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-26', '12:00:00', 'EN_TRANSITO', 12.0, 0.04, 450.00, 'Sucursal Alpha - Ciudad de México', '19.4326,-99.1332', {'Estufa de Inducción': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-26', (UUID)a7a7a7a7-a7a7-47a7-87a7-a7a7a7a7a7a7, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000001, '12:00:00', 'EN_TRANSITO', 12.0, 0.04, 450.00, 'Sucursal Alpha - Ciudad de México', '19.4326,-99.1332', {'Estufa de Inducción': 1});

-- Movimiento 8 (Delta -> Beta): Compresor de Aire Portátil
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)b8b8b8b8-b8b8-48b8-88b8-b8b8b8b8b8b8, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000004, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-27', '10:00:00', 'EN_TRANSITO', 10.0, 0.05, 180.00, 'Sucursal Beta - Guadalajara', '20.6736,-103.3406', {'Compresor de Aire Portátil': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-27', (UUID)b8b8b8b8-b8b8-48b8-88b8-b8b8b8b8b8b8, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '10:00:00', 'EN_TRANSITO', 10.0, 0.05, 180.00, 'Sucursal Beta - Guadalajara', '20.6736,-103.3406', {'Compresor de Aire Portátil': 1});

-- Movimiento 9 (Alpha -> Delta): Impresora Multifuncional
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)c9c9c9c9-c9c9-49c9-89c9-c9c9c9c9c9c9, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000001, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '2025-05-28', '11:00:00', 'EN_TRANSITO', 8.0, 0.03, 120.00, 'Sucursal Delta - Puebla', '19.0414,-98.2063', {'Impresora Multifuncional': 2});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000001, '2025-05-28', (UUID)c9c9c9c9-c9c9-49c9-89c9-c9c9c9c9c9c9, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000004, '11:00:00', 'EN_TRANSITO', 8.0, 0.03, 120.00, 'Sucursal Delta - Puebla', '19.0414,-98.2063', {'Impresora Multifuncional': 2});

-- Movimiento 10 (Gamma -> Beta): Lavadora Carga Frontal
INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d0d0d0d0-d0d0-40d0-80d0-d0d0d0d0d0d0, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000003, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '2025-05-29', '13:00:00', 'EN_TRANSITO', 60.0, 0.6, 600.00, 'Sucursal Beta - Guadalajara', '20.6736,-103.3406', {'Lavadora Carga Frontal': 1});

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
VALUES ((UUID)d1a1b1c1-1001-4111-8111-000000000003, '2025-05-29', (UUID)d0d0d0d0-d0d0-40d0-80d0-d0d0d0d0d0d0, NULL, (UUID)d1a1b1c1-1001-4111-8111-000000000002, '13:00:00', 'EN_TRANSITO', 60.0, 0.6, 600.00, 'Sucursal Beta - Guadalajara', '20.6736,-103.3406', {'Lavadora Carga Frontal': 1});
