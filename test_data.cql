-- DATOS DE PRUEBA AMPLIADOS PARA EL ESQUEMA DE INVENTARIO Y LOGÍSTICA EN CASSANDRA

-- Selecciona el Keyspace
USE inventario_logistica;

---

-- 1. Insertar datos en 'articulos_por_sucursal'
-- Vamos a simular 4 sucursales y algunos artículos en cada una.

-- Sucursal S001 (Nodo Maestro)
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion)
VALUES ('S001', 'LAPTOP_X1', 'Portatil Gamer X1', 'Portátil de alto rendimiento para juegos', 50, 2, 1000, toTimestamp(now()));

INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion)
VALUES ('S001', 'MONITOR_27', 'Monitor Curvo 27"', 'Monitor con gran inmersión visual', 120, 1, 1000, toTimestamp(now()));

INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion)
VALUES ('S001', 'TECLADO_MEC', 'Teclado Mecánico RGB', 'Teclado con switches mecánicos y retroiluminación RGB', 80, 0, 1000, toTimestamp(now()));

-- Sucursal S002
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion)
VALUES ('S002', 'IMPRESORA_LASER', 'Impresora Laser Multifunción', 'Impresora para uso de oficina con múltiples funciones', 30, 3, 800, toTimestamp(now()));

INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion)
VALUES ('S002', 'PROYECTOR_HD', 'Proyector HD', 'Proyector de alta definición para presentaciones o cine en casa', 15, 5, 800, toTimestamp(now()));

-- Sucursal S003
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion)
VALUES ('S003', 'DISCO_SSD_1TB', 'Unidad de estado sólido de alta velocidad', 'Almacenamiento rápido y eficiente', 200, 0, 1200, toTimestamp(now()));

INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion)
VALUES ('S003', 'ROUTER_WIFI6', 'Router Wi-Fi 6', 'Router de última generación para conexión rápida y estable', 40, 1, 1200, toTimestamp(now()));

-- Sucursal S004
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion)
VALUES ('S004', 'CAMARA_IP_EXT', 'Cámara IP Exterior', 'Cámara de seguridad para exteriores con visión nocturna', 75, 1, 900, toTimestamp(now()));

INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion)
VALUES ('S004', 'DRON_BEGINNER', 'Dron para principiantes', 'Dron fácil de volar con cámara HD', 25, 4, 900, toTimestamp(now()));

-- 40 Artículos adicionales distribuidos entre sucursales
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S001', 'HEADSET_GAMER', 'Auriculares Gamer Pro', 'Auriculares con sonido envolvente y micrófono cancelador de ruido', 150, 1, 1000, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S002', 'MOUSE_ERGONOMICO', 'Mouse Ergonómico', 'Mouse diseñado para la comodidad durante largas horas de uso', 200, 0, 800, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S003', 'TARJETA_VIDEO_RTX', 'Tarjeta de Video RTX 4070', 'Tarjeta gráfica de alto rendimiento para gaming y diseño', 30, 2, 1200, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S004', 'WEBCAM_4K', 'Webcam 4K Ultra HD', 'Cámara web con resolución Ultra HD para videollamadas claras', 90, 0, 900, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S001', 'MICROFONO_COND', 'Micrófono Condensador USB', 'Micrófono de estudio para grabación de voz y streaming', 70, 1, 1000, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S002', 'TABLET_ANDROID', 'Tablet Android 10"', 'Tableta con sistema operativo Android y pantalla de 10 pulgadas', 45, 2, 800, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S003', 'DISCO_EXTERNO_4TB', 'Disco Duro Externo 4TB', 'Almacenamiento portátil de gran capacidad', 100, 1, 1200, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S004', 'SMARTWATCH_GPS', 'Smartwatch con GPS', 'Reloj inteligente con funciones de seguimiento de actividad y GPS', 110, 0, 900, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S001', 'ALTAVOZ_BLUETOOTH', 'Altavoz Bluetooth Portátil', 'Altavoz compacto con conectividad Bluetooth para llevar a todas partes', 180, 1, 1000, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S002', 'POWERBANK_20K', 'Power Bank 20000mAh', 'Batería portátil de alta capacidad para cargar dispositivos móviles', 250, 0, 800, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S003', 'SSD_EXTERNO_500GB', 'SSD Externo 500GB', 'Unidad de estado sólido externa para transferencias rápidas', 130, 0, 1200, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S004', 'AURICULARES_BT_ANC', 'Auriculares BT Cancelación Ruido', 'Auriculares Bluetooth con cancelación activa de ruido', 85, 1, 900, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S001', 'USB_32GB_C', 'Memoria USB 3.0 32GB Tipo C', 'Unidad flash USB de 32GB con conector Tipo C', 300, 0, 1000, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S002', 'ADAPTADOR_HDMI_USB', 'Adaptador HDMI a USB-C', 'Adaptador para conectar dispositivos HDMI a puertos USB-C', 170, 0, 800, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S003', 'TARJETA_RED_GIGABIT', 'Tarjeta de Red Gigabit Ethernet', 'Tarjeta de red para conexión a internet de alta velocidad', 60, 0, 1200, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S004', 'FILAMENTO_PLA_1KG', 'Filamento PLA 1KG Impresión 3D', 'Filamento para impresoras 3D, 1kg de peso', 40, 1, 900, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S001', 'IMPRESORA_3D_MINI', 'Impresora 3D Mini', 'Impresora 3D compacta para uso doméstico o educativo', 10, 8, 1000, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S002', 'ESCANER_PORTATIL', 'Escáner Portátil de Documentos', 'Escáner de documentos ligero y fácil de transportar', 25, 1, 800, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S003', 'DRON_PRO_4K', 'Dron Profesional 4K', 'Dron con cámara 4K para fotografía y video aéreo', 5, 10, 1200, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S004', 'GAFAS_VR', 'Gafas de Realidad Virtual', 'Gafas para experiencias inmersivas de realidad virtual', 12, 4, 900, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S001', 'TECLADO_GAMER_MINI', 'Teclado Gamer Mini 60%', 'Teclado compacto para gaming, formato 60%', 95, 0, 1000, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S002', 'LAPTOP_OFICINA', 'Laptop Oficina Básica', 'Portátil para tareas de oficina y uso diario', 70, 2, 800, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S003', 'MONITOR_144HZ', 'Monitor 24" 144Hz', 'Monitor de 24 pulgadas con alta frecuencia de refresco', 60, 1, 1200, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S004', 'TABLET_GRAFICA', 'Tableta Gráfica Digital', 'Tableta para dibujo digital y diseño gráfico', 35, 1, 900, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S001', 'CAMARA_SEGURIDAD_CASA', 'Cámara de Seguridad Interior', 'Cámara de seguridad para el hogar con monitoreo remoto', 150, 0, 1000, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S002', 'SILLA_OFICINA_ERG', 'Silla de Oficina Ergonómica', 'Silla cómoda y ajustable para largas jornadas de trabajo', 20, 15, 800, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S003', 'ESCRITORIO_AJUSTABLE', 'Escritorio Ajustable en Altura', 'Escritorio con altura regulable para mayor ergonomía', 10, 20, 1200, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S004', 'TECLADO_BT_MULTI', 'Teclado Bluetooth Multi-dispositivo', 'Teclado inalámbrico compatible con múltiples dispositivos', 100, 0, 900, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S001', 'MOUSE_GAMER_RGB', 'Mouse Gamer RGB Ligero', 'Mouse para juegos con diseño ligero y luces RGB', 110, 0, 1000, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S002', 'SSD_M2_NVME_1TB', 'SSD M.2 NVMe 1TB', 'Unidad de estado sólido ultra rápida formato M.2', 80, 0, 800, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S003', 'MEMORIA_RAM_16GB', 'Memoria RAM DDR4 16GB', 'Módulo de memoria RAM para mejorar el rendimiento del PC', 90, 0, 1200, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S004', 'FUENTE_PODER_750W', 'Fuente de Poder 750W Certificada', 'Fuente de alimentación de 750W con certificación de eficiencia', 40, 2, 900, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S001', 'GABINETE_ATX', 'Gabinete PC ATX con Cristal Templado', 'Caja de PC con diseño moderno y panel de cristal templado', 25, 5, 1000, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S002', 'VENTILADOR_CPU', 'Ventilador CPU RGB', 'Ventilador para CPU con iluminación RGB', 120, 1, 800, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S003', 'PASTA_TERMICA', 'Pasta Térmica Alta Conductividad', 'Pasta térmica para CPUs y GPUs, mejora la disipación de calor', 300, 0, 1200, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S004', 'CABLES_MODULARES_PC', 'Cables Modulares para Fuente PC', 'Juego de cables modulares para fuentes de alimentación de PC', 150, 0, 900, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S001', 'ADAPTADOR_WIFI_USB', 'Adaptador Wi-Fi USB Doble Banda', 'Adaptador USB para conexión Wi-Fi de doble banda', 180, 0, 1000, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S002', 'EXTENSOR_RED_MESH', 'Extensor de Red Mesh Wi-Fi', 'Extensor de red para sistemas Wi-Fi Mesh', 30, 1, 800, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S003', 'SWITCH_ETHERNET_8P', 'Switch Ethernet Gigabit 8 Puertos', 'Switch de red con 8 puertos Gigabit Ethernet', 50, 1, 1200, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S004', 'CABLE_RECARGA_USB_C', 'Cable Recarga USB-C a USB-C', 'Cable de recarga USB-C a USB-C de alta velocidad', 400, 0, 900, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S001', 'CONVERTIDOR_DP_HDMI', 'Convertidor DisplayPort a HDMI', 'Adaptador para convertir señal DisplayPort a HDMI', 120, 0, 1000, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S002', 'ADAPTADOR_VGA_HDMI', 'Adaptador VGA a HDMI con Audio', 'Adaptador para convertir señal VGA a HDMI con audio', 90, 0, 800, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S003', 'TARJETA_SONIDO_USB', 'Tarjeta de Sonido Externa USB', 'Tarjeta de sonido externa para mejorar la calidad de audio', 70, 0, 1200, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S004', 'CAMARA_WEB_FULLHD', 'Cámara Web Full HD con Micrófono', 'Cámara web con resolución Full HD y micrófono incorporado', 130, 0, 900, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S001', 'MINI_PC_INTEL', 'Mini PC Intel NUC', 'Computadora compacta Intel NUC', 20, 3, 1000, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S002', 'CEREBRO_RASPBERRY', 'Raspberry Pi 4 Model B', 'Placa de desarrollo Raspberry Pi 4', 50, 0, 800, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S003', 'KIT_ARDUINO_INI', 'Kit Arduino Inicio', 'Kit de inicio para proyectos con Arduino', 80, 1, 1200, toTimestamp(now()));
INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre_articulo, descripcion, cantidad, capacidad_espacio, capacidad_max_sucursal, timestamp_ultima_actualizacion) VALUES ('S004', 'KIT_SENSORES_RASP', 'Kit de Sensores Raspberry Pi', 'Kit de sensores compatible con Raspberry Pi', 60, 0, 900, toTimestamp(now()));

---

-- 2. Insertar datos en 'clientes'

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)3d70653d-24ed-4f7f-83a3-d73187a02c31, 'Juan', 'Perez', 'Calle Falsa 123, Ciudad de México', '5512345678', 'juan.perez@example.com', '2024-01-15 10:30:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)9a0b1c2d-3e4f-5678-90ab-cdef01234567, 'Maria', 'Gomez', 'Av. Siempre Viva 742, Monterrey', '8198765432', 'maria.gomez@example.com', '2024-02-20 14:00:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)f8e7d6c5-b4a3-2109-8765-4321fedcba98, 'Carlos', 'Ruiz', 'Blvd. de los Sueños 45, Guadalajara', '3367890123', 'carlos.ruiz@example.com', '2024-03-01 09:15:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)f1a2b3c4-d5e6-4890-a234-567890abcdef, 'Ana', 'Lopez', 'Paseo de la Reforma 100, CDMX', '5523456789', 'ana.lopez@example.com', '2024-03-10 11:00:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)e1f2a3b4-c5d6-4e78-f901-23456789abcd, 'Luis', 'Hernandez', 'Avenida Insurgentes 500, CDMX', '5534567890', 'luis.hernandez@example.com', '2024-03-12 13:45:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)d1c2b3a4-e5f6-4789-b012-34567890efab, 'Sofia', 'Martinez', 'Calle del Sol 25, Guadalajara', '3378901234', 'sofia.martinez@example.com', '2024-03-15 16:20:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)c1b2a3d4-e5f6-4901-c234-567890abcdef, 'Ricardo', 'Garcia', 'Morelos 300, Monterrey', '8145678901', 'ricardo.garcia@example.com', '2024-03-18 09:50:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)b1a2c3d4-e5f6-4012-d345-67890abcdef0, 'Laura', 'Rodriguez', 'Juárez 10, Puebla', '2221234567', 'laura.rodriguez@example.com', '2024-03-20 10:10:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)a1b2c3d4-e5f6-4123-e456-7890abcdef01, 'Fernando', 'Sanchez', 'Constitución 50, Querétaro', '4421234567', 'fernando.sanchez@example.com', '2024-03-22 14:00:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)1a2b3c4d-5e6f-4234-a567-890abcdef012, 'Gabriela', 'Diaz', '5 de Mayo 20, Tijuana', '6641234567', 'gabriela.diaz@example.com', '2024-03-25 11:30:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)2a3b4c5d-6e7f-4345-b678-90abcdef0123, 'Manuel', 'Torres', 'Benito Juarez 15, León', '4771234567', 'manuel.torres@example.com', '2024-03-28 17:00:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)3a4b5c6d-7e8f-4456-c789-0abcdef01234, 'Valentina', 'Flores', 'Miguel Hidalgo 88, Cancún', '9981234567', 'valentina.flores@example.com', '2024-03-30 08:00:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)4a5b6c7d-8e9f-4567-d890-1234567890ab, 'Diego', 'Ramirez', 'Niños Heroes 11, Mérida', '9991234567', 'diego.ramirez@example.com', '2024-04-01 10:45:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)5a6b7c8d-9e0f-4678-e901-234567890abc, 'Isabel', 'Vargas', 'Ignacio Zaragoza 99, Chihuahua', '6141234567', 'isabel.vargas@example.com', '2024-04-03 14:10:00');

INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email, fecha_registro)
VALUES ((UUID)6a7b8c9d-0e1f-4789-f012-34567890abcd, 'Jorge', 'Morales', 'Francisco I. Madero 77, Tijuana', '6649876543', 'jorge.morales@example.com', '2024-04-05 16:55:00');

---

-- 3. Insertar datos en 'guias_envio_por_id' y 'guias_envio_por_sucursal_fecha'

-- Venta 1: Sucursal S001, Cliente Juan Perez, Laptop Gamer X1
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio)
VALUES ((UUID)1c4e7f8b-9d0c-4e1a-b2c3-d4e5f67890ab, 'LAPTOP_X1', 'Portatil Gamer X1', 1, 'S001', (UUID)3d70653d-24ed-4f7f-83a3-d73187a02c31, '2025-05-20 11:00:00', 'ENTREGADO');

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio)
VALUES ('S001', '2025-05-20', (UUID)1c4e7f8b-9d0c-4e1a-b2c3-d4e5f67890ab, 'LAPTOP_X1', 'Portatil Gamer X1', 1, (UUID)3d70653d-24ed-4f7f-83a3-d73187a02c31, 'ENTREGADO');

---

-- Venta 2: Sucursal S001, Cliente Maria Gomez, Monitor Curvo 27"
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio)
VALUES ((UUID)2f3b4c5d-6e7a-4891-b2c3-d4e5f67890ab, 'MONITOR_27', 'Monitor Curvo 27"', 2, 'S001', (UUID)9a0b1c2d-3e4f-5678-90ab-cdef01234567, '2025-05-22 15:30:00', 'EN_TRANSITO');

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio)
VALUES ('S001', '2025-05-22', (UUID)2f3b4c5d-6e7a-4891-b2c3-d4e5f67890ab, 'MONITOR_27', 'Monitor Curvo 27"', 2, (UUID)9a0b1c2d-3e4f-5678-90ab-cdef01234567, 'EN_TRANSITO');

---

-- Venta 3: Sucursal S002, Cliente Carlos Ruiz, Impresora Laser Multifunción
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio)
VALUES ((UUID)3a4b5c6d-7e8f-4902-c3d4-e5f6789012ab, 'IMPRESORA_LASER', 'Impresora Laser Multifunción', 1, 'S002', (UUID)f8e7d6c5-b4a3-2109-8765-4321fedcba98, '2025-05-24 09:00:00', 'PENDIENTE');

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio)
VALUES ('S002', '2025-05-24', (UUID)3a4b5c6d-7e8f-4902-c3d4-e5f6789012ab, 'IMPRESORA_LASER', 'Impresora Laser Multifunción', 1, (UUID)f8e7d6c5-b4a3-2109-8765-4321fedcba98, 'PENDIENTE');

---

-- Venta 4: Sucursal S003, Cliente Juan Perez, Disco SSD 1TB
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio)
VALUES ((UUID)4b5c6d7e-8f9a-4013-d4e5-f678901234cd, 'DISCO_SSD_1TB', 'Disco SSD 1TB', 3, 'S003', (UUID)3d70653d-24ed-4f7f-83a3-d73187a02c31, '2025-05-23 16:45:00', 'ENTREGADO');

INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio)
VALUES ('S003', '2025-05-23', (UUID)4b5c6d7e-8f9a-4013-d4e5-f678901234cd, 'DISCO_SSD_1TB', 'Disco SSD 1TB', 3, (UUID)3d70653d-24ed-4f7f-83a3-d73187a02c31, 'ENTREGADO');

---

-- 20 Ventas adicionales
-- Venta 5: S001, Cliente Ana, Teclado Mecánico RGB
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)5c6d7e8f-9a0b-4124-e5f6-7890123456ef, 'TECLADO_MEC', 'Teclado Mecánico RGB', 1, 'S001', (UUID)f1a2b3c4-d5e6-4890-a234-567890abcdef, '2025-05-24 10:15:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-05-24', (UUID)5c6d7e8f-9a0b-4124-e5f6-7890123456ef, 'TECLADO_MEC', 'Teclado Mecánico RGB', 1, (UUID)f1a2b3c4-d5e6-4890-a234-567890abcdef, 'PENDIENTE');

-- Venta 6: S002, Cliente Luis, Impresora Laser Multifunción
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)6d7e8f9a-0b1c-4235-f678-9012345678ab, 'IMPRESORA_LASER', 'Impresora Laser Multifunción', 1, 'S002', (UUID)e1f2a3b4-c5d6-4e78-f901-23456789abcd, '2025-05-24 11:30:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-05-24', (UUID)6d7e8f9a-0b1c-4235-f678-9012345678ab, 'IMPRESORA_LASER', 'Impresora Laser Multifunción', 1, (UUID)e1f2a3b4-c5d6-4e78-f901-23456789abcd, 'EN_TRANSITO');

-- Venta 7: S003, Cliente Sofia, Router Wi-Fi 6
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)7e8f9a0b-1c2d-4346-a789-0123456789bc, 'ROUTER_WIFI6', 'Router Wi-Fi 6', 1, 'S003', (UUID)d1c2b3a4-e5f6-4789-b012-34567890efab, '2025-05-23 09:45:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S003', '2025-05-23', (UUID)7e8f9a0b-1c2d-4346-a789-0123456789bc, 'ROUTER_WIFI6', 'Router Wi-Fi 6', 1, (UUID)d1c2b3a4-e5f6-4789-b012-34567890efab, 'ENTREGADO');

-- Venta 8: S004, Cliente Ricardo, Cámara IP Exterior
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)8f9a0b1c-2d3e-4457-b890-1234567890cd, 'CAMARA_IP_EXT', 'Cámara IP Exterior', 1, 'S004', (UUID)c1b2a3d4-e5f6-4901-c234-567890abcdef, '2025-05-22 14:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S004', '2025-05-22', (UUID)8f9a0b1c-2d3e-4457-b890-1234567890cd, 'CAMARA_IP_EXT', 'Cámara IP Exterior', 1, (UUID)c1b2a3d4-e5f6-4901-c234-567890abcdef, 'ENTREGADO');

-- Venta 9: S001, Cliente Laura, Headset Gamer Pro
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)9a0b1c2d-3e4f-4568-c901-2345678901de, 'HEADSET_GAMER', 'Auriculares Gamer Pro', 1, 'S001', (UUID)b1a2c3d4-e5f6-4012-d345-67890abcdef0, '2025-05-25 10:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-05-25', (UUID)9a0b1c2d-3e4f-4568-c901-2345678901de, 'HEADSET_GAMER', 'Auriculares Gamer Pro', 1, (UUID)b1a2c3d4-e5f6-4012-d345-67890abcdef0, 'PENDIENTE');

-- Venta 10: S002, Cliente Fernando, Mouse Ergonómico
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)0b1c2d3e-4f5a-4679-d012-3456789012ef, 'MOUSE_ERGONOMICO', 'Mouse Ergonómico', 1, 'S002', (UUID)a1b2c3d4-e5f6-4123-e456-7890abcdef01, '2025-05-25 11:45:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-05-25', (UUID)0b1c2d3e-4f5a-4679-d012-3456789012ef, 'MOUSE_ERGONOMICO', 'Mouse Ergonómico', 1, (UUID)a1b2c3d4-e5f6-4123-e456-7890abcdef01, 'EN_TRANSITO');

-- Venta 11: S003, Cliente Gabriela, Tarjeta de Video RTX 4070
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)1c2d3e4f-5a6b-4780-e123-4567890123fa, 'TARJETA_VIDEO_RTX', 'Tarjeta de Video RTX 4070', 1, 'S003', (UUID)1a2b3c4d-5e6f-4234-a567-890abcdef012, '2025-05-25 13:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S003', '2025-05-25', (UUID)1c2d3e4f-5a6b-4780-e123-4567890123fa, 'TARJETA_VIDEO_RTX', 'Tarjeta de Video RTX 4070', 1, (UUID)1a2b3c4d-5e6f-4234-a567-890abcdef012, 'PENDIENTE');

-- Venta 12: S004, Cliente Manuel, Webcam 4K Ultra HD
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)2d3e4f5a-6b7c-4891-f234-5678901234bc, 'WEBCAM_4K', 'Webcam 4K Ultra HD', 1, 'S004', (UUID)2a3b4c5d-6e7f-4345-b678-90abcdef0123, '2025-05-25 15:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S004', '2025-05-25', (UUID)2d3e4f5a-6b7c-4891-f234-5678901234bc, 'WEBCAM_4K', 'Webcam 4K Ultra HD', 1, (UUID)2a3b4c5d-6e7f-4345-b678-90abcdef0123, 'ENTREGADO');

-- Venta 13: S001, Cliente Valentina, Micrófono Condensador USB
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)3e4f5a6b-7c8d-4902-a345-6789012345cd, 'MICROFONO_COND', 'Micrófono Condensador USB', 1, 'S001', (UUID)3a4b5c6d-7e8f-4456-c789-0abcdef01234, '2025-05-26 09:30:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-05-26', (UUID)3e4f5a6b-7c8d-4902-a345-6789012345cd, 'MICROFONO_COND', 'Micrófono Condensador USB', 1, (UUID)3a4b5c6d-7e8f-4456-c789-0abcdef01234, 'PENDIENTE');

-- Venta 14: S002, Cliente Diego, Tablet Android 10"
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)4f5a6b7c-8d9e-4013-b456-7890123456de, 'TABLET_ANDROID', 'Tablet Android 10"', 1, 'S002', (UUID)4a5b6c7d-8e9f-4567-d890-1234567890ab, '2025-05-26 12:00:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-05-26', (UUID)4f5a6b7c-8d9e-4013-b456-7890123456de, 'TABLET_ANDROID', 'Tablet Android 10"', 1, (UUID)4a5b6c7d-8e9f-4567-d890-1234567890ab, 'EN_TRANSITO');

-- Venta 15: S003, Cliente Isabel, Disco Duro Externo 4TB
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)5a6b7c8d-9e0f-4124-c567-8901234567ef, 'DISCO_EXTERNO_4TB', 'Disco Duro Externo 4TB', 1, 'S003', (UUID)5a6b7c8d-9e0f-4678-e901-234567890abc, '2025-05-26 14:30:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S003', '2025-05-26', (UUID)5a6b7c8d-9e0f-4124-c567-8901234567ef, 'DISCO_EXTERNO_4TB', 'Disco Duro Externo 4TB', 1, (UUID)5a6b7c8d-9e0f-4678-e901-234567890abc, 'ENTREGADO');

-- Venta 16: S004, Cliente Jorge, Smartwatch con GPS
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)6b7c8d9e-0f1a-4235-d678-9012345678fa, 'SMARTWATCH_GPS', 'Smartwatch con GPS', 1, 'S004', (UUID)6a7b8c9d-0e1f-4789-f012-34567890abcd, '2025-05-26 16:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S004', '2025-05-26', (UUID)6b7c8d9e-0f1a-4235-d678-9012345678fa, 'SMARTWATCH_GPS', 'Smartwatch con GPS', 1, (UUID)6a7b8c9d-0e1f-4789-f012-34567890abcd, 'PENDIENTE');

-- Venta 17: S001, Cliente Juan Perez, Altavoz Bluetooth Portátil
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)7c8d9e0f-1a2b-4346-e789-0123456789ab, 'ALTAVOZ_BLUETOOTH', 'Altavoz Bluetooth Portátil', 1, 'S001', (UUID)3d70653d-24ed-4f7f-83a3-d73187a02c31, '2025-05-27 10:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-05-27', (UUID)7c8d9e0f-1a2b-4346-e789-0123456789ab, 'ALTAVOZ_BLUETOOTH', 'Altavoz Bluetooth Portátil', 1, (UUID)3d70653d-24ed-4f7f-83a3-d73187a02c31, 'ENTREGADO');

-- Venta 18: S002, Cliente Maria Gomez, Power Bank 20000mAh
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)8d9e0f1a-2b3c-4457-f890-1234567890bc, 'POWERBANK_20K', 'Power Bank 20000mAh', 2, 'S002', (UUID)9a0b1c2d-3e4f-5678-90ab-cdef01234567, '2025-05-27 12:00:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-05-27', (UUID)8d9e0f1a-2b3c-4457-f890-1234567890bc, 'POWERBANK_20K', 'Power Bank 20000mAh', 2, (UUID)9a0b1c2d-3e4f-5678-90ab-cdef01234567, 'EN_TRANSITO');

-- Venta 19: S003, Cliente Carlos Ruiz, SSD Externo 500GB
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)9e0f1a2b-3c4d-4568-a901-2345678901cd, 'SSD_EXTERNO_500GB', 'SSD Externo 500GB', 1, 'S003', (UUID)f8e7d6c5-b4a3-2109-8765-4321fedcba98, '2025-05-27 14:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S003', '2025-05-27', (UUID)9e0f1a2b-3c4d-4568-a901-2345678901cd, 'SSD_EXTERNO_500GB', 'SSD Externo 500GB', 1, (UUID)f8e7d6c5-b4a3-2109-8765-4321fedcba98, 'PENDIENTE');

-- Venta 20: S004, Cliente Ana Lopez, Auriculares BT Cancelación Ruido
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)0f1a2b3c-4d5e-4679-b012-3456789012de, 'AURICULARES_BT_ANC', 'Auriculares BT Cancelación Ruido', 1, 'S004', (UUID)f1a2b3c4-d5e6-4890-a234-567890abcdef, '2025-05-27 16:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S004', '2025-05-27', (UUID)0f1a2b3c-4d5e-4679-b012-3456789012de, 'AURICULARES_BT_ANC', 'Auriculares BT Cancelación Ruido', 1, (UUID)f1a2b3c4-d5e6-4890-a234-567890abcdef, 'ENTREGADO');

-- Venta 21: S001, Cliente Luis Hernandez, Memoria USB 3.0 32GB Tipo C
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)1a2b3c4d-5e6f-4780-c123-4567890123ef, 'USB_32GB_C', 'Memoria USB 3.0 32GB Tipo C', 2, 'S001', (UUID)e1f2a3b4-c5d6-4e78-f901-23456789abcd, '2025-05-28 09:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-05-28', (UUID)1a2b3c4d-5e6f-4780-c123-4567890123ef, 'USB_32GB_C', 'Memoria USB 3.0 32GB Tipo C', 2, (UUID)e1f2a3b4-c5d6-4e78-f901-23456789abcd, 'PENDIENTE');

-- Venta 22: S002, Cliente Sofia Martinez, Adaptador HDMI a USB-C
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)2b3c4d5e-6f7a-4891-d234-5678901234fa, 'ADAPTADOR_HDMI_USB', 'Adaptador HDMI a USB-C', 1, 'S002', (UUID)d1c2b3a4-e5f6-4789-b012-34567890efab, '2025-05-28 11:00:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-05-28', (UUID)2b3c4d5e-6f7a-4891-d234-5678901234fa, 'ADAPTADOR_HDMI_USB', 'Adaptador HDMI a USB-C', 1, (UUID)d1c2b3a4-e5f6-4789-b012-34567890efab, 'EN_TRANSITO');

-- Venta 23: S003, Cliente Ricardo Garcia, Tarjeta de Red Gigabit Ethernet
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)3c4d5e6f-7a8b-4902-e345-6789012345ab, 'TARJETA_RED_GIGABIT', 'Tarjeta de Red Gigabit Ethernet', 1, 'S003', (UUID)c1b2a3d4-e5f6-4901-c234-567890abcdef, '2025-05-28 13:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S003', '2025-05-28', (UUID)3c4d5e6f-7a8b-4902-e345-6789012345ab, 'TARJETA_RED_GIGABIT', 'Tarjeta de Red Gigabit Ethernet', 1, (UUID)c1b2a3d4-e5f6-4901-c234-567890abcdef, 'ENTREGADO');

-- Venta 24: S004, Cliente Laura Rodriguez, Filamento PLA 1KG Impresión 3D
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)4d5e6f7a-8b9c-4013-f456-7890123456bc, 'FILAMENTO_PLA_1KG', 'Filamento PLA 1KG Impresión 3D', 2, 'S004', (UUID)b1a2c3d4-e5f6-4012-d345-67890abcdef0, '2025-05-28 15:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S004', '2025-05-28', (UUID)4d5e6f7a-8b9c-4013-f456-7890123456bc, 'FILAMENTO_PLA_1KG', 'Filamento PLA 1KG Impresión 3D', 2, (UUID)b1a2c3d4-e5f6-4012-d345-67890abcdef0, 'PENDIENTE');

-- Venta 25: S001, Cliente Fernando Sanchez, Impresora 3D Mini
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)5e6f7a8b-9c0d-4124-a567-8901234567cd, 'IMPRESORA_3D_MINI', 'Impresora 3D Mini', 1, 'S001', (UUID)a1b2c3d4-e5f6-4123-e456-7890abcdef01, '2025-05-29 10:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-05-29', (UUID)5e6f7a8b-9c0d-4124-a567-8901234567cd, 'IMPRESORA_3D_MINI', 'Impresora 3D Mini', 1, (UUID)a1b2c3d4-e5f6-4123-e456-7890abcdef01, 'ENTREGADO');

-- Venta 26: S002, Cliente Gabriela Diaz, Escáner Portátil de Documentos
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)6f7a8b9c-0d1e-4235-b678-9012345678de, 'ESCANER_PORTATIL', 'Escáner Portátil de Documentos', 1, 'S002', (UUID)1a2b3c4d-5e6f-4234-a567-890abcdef012, '2025-05-29 12:00:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-05-29', (UUID)6f7a8b9c-0d1e-4235-b678-9012345678de, 'ESCANER_PORTATIL', 'Escáner Portátil de Documentos', 1, (UUID)1a2b3c4d-5e6f-4234-a567-890abcdef012, 'EN_TRANSITO');

-- Venta 27: S003, Cliente Manuel Torres, Dron Profesional 4K
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)7a8b9c0d-1e2f-4346-c789-0123456789ef, 'DRON_PRO_4K', 'Dron Profesional 4K', 1, 'S003', (UUID)2a3b4c5d-6e7f-4345-b678-90abcdef0123, '2025-05-29 14:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S003', '2025-05-29', (UUID)7a8b9c0d-1e2f-4346-c789-0123456789ef, 'DRON_PRO_4K', 'Dron Profesional 4K', 1, (UUID)2a3b4c5d-6e7f-4345-b678-90abcdef0123, 'PENDIENTE');

-- Venta 28: S004, Cliente Valentina Flores, Gafas de Realidad Virtual
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)8b9c0d1e-2f3a-4457-d890-1234567890fa, 'GAFAS_VR', 'Gafas de Realidad Virtual', 1, 'S004', (UUID)3a4b5c6d-7e8f-4456-c789-0abcdef01234, '2025-05-29 16:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S004', '2025-05-29', (UUID)8b9c0d1e-2f3a-4457-d890-1234567890fa, 'GAFAS_VR', 'Gafas de Realidad Virtual', 1, (UUID)3a4b5c6d-7e8f-4456-c789-0abcdef01234, 'ENTREGADO');

-- Venta 29: S001, Cliente Diego Ramirez, Teclado Gamer Mini 60%
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)9c0d1e2f-3a4b-4568-e901-2345678901ab, 'TECLADO_GAMER_MINI', 'Teclado Gamer Mini 60%', 1, 'S001', (UUID)4a5b6c7d-8e9f-4567-d890-1234567890ab, '2025-05-30 09:30:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-05-30', (UUID)9c0d1e2f-3a4b-4568-e901-2345678901ab, 'TECLADO_GAMER_MINI', 'Teclado Gamer Mini 60%', 1, (UUID)4a5b6c7d-8e9f-4567-d890-1234567890ab, 'PENDIENTE');

-- Venta 30: S002, Cliente Isabel Vargas, Laptop Oficina Básica
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)0d1e2f3a-4b5c-4679-f012-3456789012bc, 'LAPTOP_OFICINA', 'Laptop Oficina Básica', 1, 'S002', (UUID)5a6b7c8d-9e0f-4678-e901-234567890abc, '2025-05-30 11:00:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-05-30', (UUID)0d1e2f3a-4b5c-4679-f012-3456789012bc, 'LAPTOP_OFICINA', 'Laptop Oficina Básica', 1, (UUID)5a6b7c8d-9e0f-4678-e901-234567890abc, 'EN_TRANSITO');

-- Venta 31: S003, Cliente Jorge Morales, Monitor 24" 144Hz
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)1e2f3a4b-5c6d-4780-a123-4567890123cd, 'MONITOR_144HZ', 'Monitor 24" 144Hz', 1, 'S003', (UUID)6a7b8c9d-0e1f-4789-f012-34567890abcd, '2025-05-30 13:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S003', '2025-05-30', (UUID)1e2f3a4b-5c6d-4780-a123-4567890123cd, 'MONITOR_144HZ', 'Monitor 24" 144Hz', 1, (UUID)6a7b8c9d-0e1f-4789-f012-34567890abcd, 'ENTREGADO');

-- Venta 32: S004, Cliente Juan Perez, Tableta Gráfica Digital
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)2f3a4b5c-6d7e-4891-b234-5678901234de, 'TABLET_GRAFICA', 'Tableta Gráfica Digital', 1, 'S004', (UUID)3d70653d-24ed-4f7f-83a3-d73187a02c31, '2025-05-30 15:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S004', '2025-05-30', (UUID)2f3a4b5c-6d7e-4891-b234-5678901234de, 'TABLET_GRAFICA', 'Tableta Gráfica Digital', 1, (UUID)3d70653d-24ed-4f7f-83a3-d73187a02c31, 'PENDIENTE');

-- Venta 33: S001, Cliente Maria Gomez, Cámara de Seguridad Interior
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)3a4b5c6d-7e8f-4902-c345-6789012345ef, 'CAMARA_SEGURIDAD_CASA', 'Cámara de Seguridad Interior', 1, 'S001', (UUID)9a0b1c2d-3e4f-5678-90ab-cdef01234567, '2025-05-31 09:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-05-31', (UUID)3a4b5c6d-7e8f-4902-c345-6789012345ef, 'CAMARA_SEGURIDAD_CASA', 'Cámara de Seguridad Interior', 1, (UUID)9a0b1c2d-3e4f-5678-90ab-cdef01234567, 'ENTREGADO');

-- Venta 34: S002, Cliente Carlos Ruiz, Silla de Oficina Ergonómica
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)4b5c6d7e-8f9a-4013-d456-7890123456fa, 'SILLA_OFICINA_ERG', 'Silla de Oficina Ergonómica', 1, 'S002', (UUID)f8e7d6c5-b4a3-2109-8765-4321fedcba98, '2025-05-31 11:00:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-05-31', (UUID)4b5c6d7e-8f9a-4013-d456-7890123456fa, 'SILLA_OFICINA_ERG', 'Silla de Oficina Ergonómica', 1, (UUID)f8e7d6c5-b4a3-2109-8765-4321fedcba98, 'EN_TRANSITO');

-- Venta 35: S003, Cliente Ana Lopez, Escritorio Ajustable en Altura
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)5c6d7e8f-9a0b-4124-e567-8901234567ab, 'ESCRITORIO_AJUSTABLE', 'Escritorio Ajustable en Altura', 1, 'S003', (UUID)f1a2b3c4-d5e6-4890-a234-567890abcdef, '2025-05-31 13:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S003', '2025-05-31', (UUID)5c6d7e8f-9a0b-4124-e567-8901234567ab, 'ESCRITORIO_AJUSTABLE', 'Escritorio Ajustable en Altura', 1, (UUID)f1a2b3c4-d5e6-4890-a234-567890abcdef, 'PENDIENTE');

-- Venta 36: S004, Cliente Luis Hernandez, Teclado Bluetooth Multi-dispositivo
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)6d7e8f9a-0b1c-4235-f678-9012345678cd, 'TECLADO_BT_MULTI', 'Teclado Bluetooth Multi-dispositivo', 1, 'S004', (UUID)e1f2a3b4-c5d6-4e78-f901-23456789abcd, '2025-05-31 15:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S004', '2025-05-31', (UUID)6d7e8f9a-0b1c-4235-f678-9012345678cd, 'TECLADO_BT_MULTI', 'Teclado Bluetooth Multi-dispositivo', 1, (UUID)e1f2a3b4-c5d6-4e78-f901-23456789abcd, 'ENTREGADO');

-- Venta 37: S001, Cliente Sofia Martinez, Mouse Gamer RGB Ligero
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)7e8f9a0b-1c2d-4346-a789-0123456789de, 'MOUSE_GAMER_RGB', 'Mouse Gamer RGB Ligero', 1, 'S001', (UUID)d1c2b3a4-e5f6-4789-b012-34567890efab, '2025-06-01 10:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-06-01', (UUID)7e8f9a0b-1c2d-4346-a789-0123456789de, 'MOUSE_GAMER_RGB', 'Mouse Gamer RGB Ligero', 1, (UUID)d1c2b3a4-e5f6-4789-b012-34567890efab, 'PENDIENTE');

-- Venta 38: S002, Cliente Ricardo Garcia, SSD M.2 NVMe 1TB
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)8f9a0b1c-2d3e-4457-b890-1234567890ef, 'SSD_M2_NVME_1TB', 'SSD M.2 NVMe 1TB', 1, 'S002', (UUID)c1b2a3d4-e5f6-4901-c234-567890abcdef, '2025-06-01 12:00:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-06-01', (UUID)8f9a0b1c-2d3e-4457-b890-1234567890ef, 'SSD_M2_NVME_1TB', 'SSD M.2 NVMe 1TB', 1, (UUID)c1b2a3d4-e5f6-4901-c234-567890abcdef, 'EN_TRANSITO');

-- Venta 39: S003, Cliente Laura Rodriguez, Memoria RAM DDR4 16GB
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)9a0b1c2d-3e4f-4568-c901-2345678901fa, 'MEMORIA_RAM_16GB', 'Memoria RAM DDR4 16GB', 2, 'S003', (UUID)b1a2c3d4-e5f6-4012-d345-67890abcdef0, '2025-06-01 14:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S003', '2025-06-01', (UUID)9a0b1c2d-3e4f-4568-c901-2345678901fa, 'MEMORIA_RAM_16GB', 'Memoria RAM DDR4 16GB', 2, (UUID)b1a2c3d4-e5f6-4012-d345-67890abcdef0, 'ENTREGADO');

-- Venta 40: S004, Cliente Fernando Sanchez, Fuente de Poder 750W Certificada
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)0b1c2d3e-4f5a-4679-d012-3456789012ab, 'FUENTE_PODER_750W', 'Fuente de Poder 750W Certificada', 1, 'S004', (UUID)a1b2c3d4-e5f6-4123-e456-7890abcdef01, '2025-06-01 16:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S004', '2025-06-01', (UUID)0b1c2d3e-4f5a-4679-d012-3456789012ab, 'FUENTE_PODER_750W', 'Fuente de Poder 750W Certificada', 1, (UUID)a1b2c3d4-e5f6-4123-e456-7890abcdef01, 'PENDIENTE');

-- Venta 41: S001, Cliente Gabriela Diaz, Gabinete PC ATX con Cristal Templado
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)1c2d3e4f-5a6b-4780-e123-4567890123bc, 'GABINETE_ATX', 'Gabinete PC ATX con Cristal Templado', 1, 'S001', (UUID)1a2b3c4d-5e6f-4234-a567-890abcdef012, '2025-06-02 09:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-06-02', (UUID)1c2d3e4f-5a6b-4780-e123-4567890123bc, 'GABINETE_ATX', 'Gabinete PC ATX con Cristal Templado', 1, (UUID)1a2b3c4d-5e6f-4234-a567-890abcdef012, 'ENTREGADO');

-- Venta 42: S002, Cliente Manuel Torres, Ventilador CPU RGB
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)2d3e4f5a-6b7c-4891-f234-5678901234cd, 'VENTILADOR_CPU', 'Ventilador CPU RGB', 1, 'S002', (UUID)2a3b4c5d-6e7f-4345-b678-90abcdef0123, '2025-06-02 11:00:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-06-02', (UUID)2d3e4f5a-6b7c-4891-f234-5678901234cd, 'VENTILADOR_CPU', 'Ventilador CPU RGB', 1, (UUID)2a3b4c5d-6e7f-4345-b678-90abcdef0123, 'EN_TRANSITO');

-- Venta 43: S003, Cliente Valentina Flores, Pasta Térmica Alta Conductividad
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)3e4f5a6b-7c8d-4902-a345-6789012345de, 'PASTA_TERMICA', 'Pasta Térmica Alta Conductividad', 1, 'S003', (UUID)3a4b5c6d-7e8f-4456-c789-0abcdef01234, '2025-06-02 13:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S003', '2025-06-02', (UUID)3e4f5a6b-7c8d-4902-a345-6789012345de, 'PASTA_TERMICA', 'Pasta Térmica Alta Conductividad', 1, (UUID)3a4b5c6d-7e8f-4456-c789-0abcdef01234, 'PENDIENTE');

-- Venta 44: S004, Cliente Diego Ramirez, Cables Modulares para Fuente PC
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)4f5a6b7c-8d9e-4013-b456-7890123456ef, 'CABLES_MODULARES_PC', 'Cables Modulares para Fuente PC', 1, 'S004', (UUID)4a5b6c7d-8e9f-4567-d890-1234567890ab, '2025-06-02 15:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S004', '2025-06-02', (UUID)4f5a6b7c-8d9e-4013-b456-7890123456ef, 'CABLES_MODULARES_PC', 'Cables Modulares para Fuente PC', 1, (UUID)4a5b6c7d-8e9f-4567-d890-1234567890ab, 'ENTREGADO');

-- Venta 45: S001, Cliente Isabel Vargas, Adaptador Wi-Fi USB Doble Banda
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)5a6b7c8d-9e0f-4124-c567-8901234567fa, 'ADAPTADOR_WIFI_USB', 'Adaptador Wi-Fi USB Doble Banda', 1, 'S001', (UUID)5a6b7c8d-9e0f-4678-e901-234567890abc, '2025-06-03 09:30:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-06-03', (UUID)5a6b7c8d-9e0f-4124-c567-8901234567fa, 'ADAPTADOR_WIFI_USB', 'Adaptador Wi-Fi USB Doble Banda', 1, (UUID)5a6b7c8d-9e0f-4678-e901-234567890abc, 'PENDIENTE');

-- Venta 46: S002, Cliente Jorge Morales, Extensor de Red Mesh Wi-Fi
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)6b7c8d9e-0f1a-4235-d678-9012345678ab, 'EXTENSOR_RED_MESH', 'Extensor de Red Mesh Wi-Fi', 1, 'S002', (UUID)6a7b8c9d-0e1f-4789-f012-34567890abcd, '2025-06-03 11:30:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-06-03', (UUID)6b7c8d9e-0f1a-4235-d678-9012345678ab, 'EXTENSOR_RED_MESH', 'Extensor de Red Mesh Wi-Fi', 1, (UUID)6a7b8c9d-0e1f-4789-f012-34567890abcd, 'EN_TRANSITO');

-- Venta 47: S003, Cliente Juan Perez, Switch Ethernet Gigabit 8 Puertos
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)7c8d9e0f-1a2b-4346-e789-0123456789bc, 'SWITCH_ETHERNET_8P', 'Switch Ethernet Gigabit 8 Puertos', 1, 'S003', (UUID)3d70653d-24ed-4f7f-83a3-d73187a02c31, '2025-06-03 13:30:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S003', '2025-06-03', (UUID)7c8d9e0f-1a2b-4346-e789-0123456789bc, 'SWITCH_ETHERNET_8P', 'Switch Ethernet Gigabit 8 Puertos', 1, (UUID)3d70653d-24ed-4f7f-83a3-d73187a02c31, 'ENTREGADO');

-- Venta 48: S004, Cliente Maria Gomez, Cable Recarga USB-C a USB-C
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)8d9e0f1a-2b3c-4457-f890-1234567890cd, 'CABLE_RECARGA_USB_C', 'Cable Recarga USB-C a USB-C', 2, 'S004', (UUID)9a0b1c2d-3e4f-5678-90ab-cdef01234567, '2025-06-03 15:30:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S004', '2025-06-03', (UUID)8d9e0f1a-2b3c-4457-f890-1234567890cd, 'CABLE_RECARGA_USB_C', 'Cable Recarga USB-C a USB-C', 2, (UUID)9a0b1c2d-3e4f-5678-90ab-cdef01234567, 'PENDIENTE');

-- Venta 49: S001, Cliente Carlos Ruiz, Convertidor DisplayPort a HDMI
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)9e0f1a2b-3c4d-4568-a901-2345678901de, 'CONVERTIDOR_DP_HDMI', 'Convertidor DisplayPort a HDMI', 1, 'S001', (UUID)f8e7d6c5-b4a3-2109-8765-4321fedcba98, '2025-06-04 10:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-06-04', (UUID)9e0f1a2b-3c4d-4568-a901-2345678901de, 'CONVERTIDOR_DP_HDMI', 'Convertidor DisplayPort a HDMI', 1, (UUID)f8e7d6c5-b4a3-2109-8765-4321fedcba98, 'ENTREGADO');

-- Venta 50: S002, Cliente Ana Lopez, Adaptador VGA a HDMI con Audio
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)0f1a2b3c-4d5e-4679-b012-3456789012ef, 'ADAPTADOR_VGA_HDMI', 'Adaptador VGA a HDMI con Audio', 1, 'S002', (UUID)f1a2b3c4-d5e6-4890-a234-567890abcdef, '2025-06-04 12:00:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-06-04', (UUID)0f1a2b3c-4d5e-4679-b012-3456789012ef, 'ADAPTADOR_VGA_HDMI', 'Adaptador VGA a HDMI con Audio', 1, (UUID)f1a2b3c4-d5e6-4890-a234-567890abcdef, 'EN_TRANSITO');

-- Venta 51: S003, Cliente Luis Hernandez, Tarjeta de Sonido Externa USB
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)1a2b3c4d-5e6f-4780-c123-4567890123fa, 'TARJETA_SONIDO_USB', 'Tarjeta de Sonido Externa USB', 1, 'S003', (UUID)e1f2a3b4-c5d6-4e78-f901-23456789abcd, '2025-06-04 14:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S003', '2025-06-04', (UUID)1a2b3c4d-5e6f-4780-c123-4567890123fa, 'TARJETA_SONIDO_USB', 'Tarjeta de Sonido Externa USB', 1, (UUID)e1f2a3b4-c5d6-4e78-f901-23456789abcd, 'PENDIENTE');

-- Venta 52: S004, Cliente Sofia Martinez, Cámara Web Full HD con Micrófono
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)2b3c4d5e-6f7a-4891-d234-5678901234ab, 'CAMARA_WEB_FULLHD', 'Cámara Web Full HD con Micrófono', 1, 'S004', (UUID)d1c2b3a4-e5f6-4789-b012-34567890efab, '2025-06-04 16:00:00', 'ENTREGADO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S004', '2025-06-04', (UUID)2b3c4d5e-6f7a-4891-d234-5678901234ab, 'CAMARA_WEB_FULLHD', 'Cámara Web Full HD con Micrófono', 1, (UUID)d1c2b3a4-e5f6-4789-b012-34567890efab, 'ENTREGADO');

-- Venta 53: S001, Cliente Ricardo Garcia, Mini PC Intel NUC
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)3c4d5e6f-7a8b-4902-e345-6789012345bc, 'MINI_PC_INTEL', 'Mini PC Intel NUC', 1, 'S001', (UUID)c1b2a3d4-e5f6-4901-c234-567890abcdef, '2025-06-05 09:00:00', 'PENDIENTE');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S001', '2025-06-05', (UUID)3c4d5e6f-7a8b-4902-e345-6789012345bc, 'MINI_PC_INTEL', 'Mini PC Intel NUC', 1, (UUID)c1b2a3d4-e5f6-4901-c234-567890abcdef, 'PENDIENTE');

-- Venta 54: S002, Cliente Laura Rodriguez, Raspberry Pi 4 Model B
INSERT INTO guias_envio_por_id (guia_id, articulo_id, nombre_articulo, cantidad_vendida, sucursal_origen_id, cliente_id, fecha_venta, estado_envio) VALUES ((UUID)4d5e6f7a-8b9c-4013-f456-7890123456cd, 'CEREBRO_RASPBERRY', 'Raspberry Pi 4 Model B', 1, 'S002', (UUID)b1a2c3d4-e5f6-4012-d345-67890abcdef0, '2025-06-05 11:00:00', 'EN_TRANSITO');
INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, articulo_id, nombre_articulo, cantidad_vendida, cliente_id, estado_envio) VALUES ('S002', '2025-06-05', (UUID)4d5e6f7a-8b9c-4013-f456-7890123456cd, 'CEREBRO_RASPBERRY', 'Raspberry Pi 4 Model B', 1, (UUID)b1a2c3d4-e5f6-4012-d345-67890abcdef0, 'EN_TRANSITO');
