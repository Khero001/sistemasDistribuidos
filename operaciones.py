# operaciones.py
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, SimpleStatement
import uuid
import datetime

class CassandraOperations:
    def __init__(self, contact_points=['127.0.0.1'], port=9042, keyspace='inventario_logistica', username=None, password=None):
        self.contact_points = contact_points
        self.port = port
        self.keyspace = keyspace
        self.username = username
        self.password = password
        self.cluster = None
        self.session = None
        self._connect()

    def _connect(self):
        try:
            if self.username and self.password:
                auth_provider = PlainTextAuthProvider(self.username, self.password)
                self.cluster = Cluster(self.contact_points, port=self.port, auth_provider=auth_provider)
            else:
                self.cluster = Cluster(self.contact_points, port=self.port)
            self.session = self.cluster.connect(self.keyspace)
            print(f"Conectado a Cassandra en {self.contact_points}:{self.port}, Keyspace: {self.keyspace}")
        except Exception as e:
            print(f"Error al conectar a Cassandra: {e}")
            raise

    def close(self):
        if self.cluster:
            self.cluster.shutdown()
            print("Conexión a Cassandra cerrada.")

    # --- Operaciones para la tabla 'sucursales' ---
    def insertar_sucursal(self, sucursal_id, nombre_sucursal, direccion_ip, ciudad):
        query = """
        INSERT INTO sucursales (sucursal_id, nombre_sucursal, direccion_ip, ciudad)
        VALUES ((UUID)?, ?, ?, ?)
        """
        try:
            self.session.execute(query, (sucursal_id, nombre_sucursal, direccion_ip, ciudad))
            print(f"Sucursal '{nombre_sucursal}' insertada/actualizada correctamente.")
            return True
        except Exception as e:
            print(f"Error al insertar/actualizar sucursal: {e}")
            return False

    def seleccionar_sucursal_por_id(self, sucursal_id):
        query = "SELECT * FROM sucursales WHERE sucursal_id = (UUID)%s" % (sucursal_id)
        try:
            row = self.session.execute(query).one()
            return row
        except Exception as e:
            print(f"Error al seleccionar sucursal por ID: {e}")
            return None

    def seleccionar_todas_sucursales(self):
        query = "SELECT * FROM sucursales"
        try:
            rows = self.session.execute(query)
            return rows
        except Exception as e:
            print(f"Error al seleccionar todas las sucursales: {e}")
            return None

    def actualizar_sucursal(self, sucursal_id, nombre_sucursal=None, direccion_ip=None, ciudad=None):
        updates = []
        params = []
        if nombre_sucursal is not None:
            updates.append("nombre_sucursal = ?")
            params.append(nombre_sucursal)
        if direccion_ip is not None:
            updates.append("direccion_ip = ?")
            params.append(direccion_ip)
        if ciudad is not None:
            updates.append("ciudad = ?")
            params.append(ciudad)

        if not updates:
            print("No hay datos para actualizar en la sucursal.")
            return False

        query = f"UPDATE sucursales SET {', '.join(updates)} WHERE sucursal_id = ?"
        params.append(sucursal_id)

        try:
            self.session.execute(query, params)
            print(f"Sucursal '{sucursal_id}' actualizada correctamente.")
            return True
        except Exception as e:
            print(f"Error al actualizar sucursal: {e}")
            return False

    def borrar_sucursal(self, sucursal_id):
        query = "DELETE FROM sucursales WHERE sucursal_id = ?"
        try:
            self.session.execute(query, (sucursal_id,))
            print(f"Sucursal '{sucursal_id}' borrada correctamente.")
            return True
        except Exception as e:
            print(f"Error al borrar sucursal: {e}")
            return False

    # --- Operaciones para la tabla 'articulos_por_sucursal' ---
    def insertar_articulo_por_sucursal(self, sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento):
        query = """
        INSERT INTO articulos_por_sucursal (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)
        VALUES ((UUID)?, (UUID)?, ?, ?, ?, ?, ?)
        """
        try:
            self.session.execute(query, (sucursal_id, articulo_id, nombre, descripcion, cantidad, unidad_medida, capacidad_almacenamiento))
            print(f"Artículo '{nombre}' en sucursal '{sucursal_id}' insertado/actualizado correctamente.")
            return True
        except Exception as e:
            print(f"Error al insertar/actualizar artículo por sucursal: {e}")
            return False

    def seleccionar_articulos_por_sucursal(self, sucursal_id):
        query = "SELECT * FROM articulos_por_sucursal WHERE sucursal_id = %s" % (sucursal_id)
        try:
            rows = self.session.execute(query)
            return rows
        except Exception as e:
            print(f"Error al seleccionar artículos por sucursal: {e}")
            return None

    def seleccionar_articulo_especifico_por_sucursal(self, sucursal_id, articulo_id):
        query = "SELECT * FROM articulos_por_sucursal WHERE sucursal_id = ? AND articulo_id = ?"
        try:
            row = self.session.execute(query, (sucursal_id, articulo_id)).one()
            return row
        except Exception as e:
            print(f"Error al seleccionar artículo específico por sucursal: {e}")
            return None

    def actualizar_articulo_por_sucursal(self, sucursal_id, articulo_id, nombre=None, descripcion=None, cantidad=None, unidad_medida=None, capacidad_almacenamiento=None):
        updates = []
        params = []
        if nombre is not None:
            updates.append("nombre = ?")
            params.append(nombre)
        if descripcion is not None:
            updates.append("descripcion = ?")
            params.append(descripcion)
        if cantidad is not None:
            updates.append("cantidad = ?")
            params.append(cantidad)
        if unidad_medida is not None:
            updates.append("unidad_medida = ?")
            params.append(unidad_medida)
        if capacidad_almacenamiento is not None:
            updates.append("capacidad_almacenamiento = ?")
            params.append(capacidad_almacenamiento)

        if not updates:
            print("No hay datos para actualizar en el artículo por sucursal.")
            return False

        query = f"UPDATE articulos_por_sucursal SET {', '.join(updates)} WHERE sucursal_id = ? AND articulo_id = ?"
        params.extend([sucursal_id, articulo_id])

        try:
            self.session.execute(query, params)
            print(f"Artículo '{articulo_id}' en sucursal '{sucursal_id}' actualizado correctamente.")
            return True
        except Exception as e:
            print(f"Error al actualizar artículo por sucursal: {e}")
            return False

    def borrar_articulo_por_sucursal(self, sucursal_id, articulo_id):
        query = "DELETE FROM articulos_por_sucursal WHERE sucursal_id = ? AND articulo_id = ?"
        try:
            self.session.execute(query, (sucursal_id, articulo_id))
            print(f"Artículo '{articulo_id}' borrado de sucursal '{sucursal_id}' correctamente.")
            return True
        except Exception as e:
            print(f"Error al borrar artículo por sucursal: {e}")
            return False

    # --- Operaciones para la tabla 'clientes' ---
    def insertar_cliente(self, cliente_id, nombre, apellido, direccion, telefono, email):
        query = """
        INSERT INTO clientes (cliente_id, nombre, apellido, direccion, telefono, email)
        VALUES ((UUID)?, ?, ?, ?, ?, ?)
        """
        try:
            self.session.execute(query, (cliente_id, nombre, apellido, direccion, telefono, email))
            print(f"Cliente '{nombre} {apellido}' insertado/actualizado correctamente.")
            return True
        except Exception as e:
            print(f"Error al insertar/actualizar cliente: {e}")
            return False

    def seleccionar_cliente_por_id(self, cliente_id):
        query = "SELECT * FROM clientes WHERE cliente_id = ?"
        try:
            row = self.session.execute(query, (cliente_id,)).one()
            return row
        except Exception as e:
            print(f"Error al seleccionar cliente por ID: {e}")
            return None

    def seleccionar_todos_clientes(self):
        query = "SELECT * FROM clientes"
        try:
            rows = self.session.execute(query)
            return rows
        except Exception as e:
            print(f"Error al seleccionar todos los clientes: {e}")
            return None

    def actualizar_cliente(self, cliente_id, nombre=None, apellido=None, direccion=None, telefono=None, email=None):
        updates = []
        params = []
        if nombre is not None:
            updates.append("nombre = ?")
            params.append(nombre)
        if apellido is not None:
            updates.append("apellido = ?")
            params.append(apellido)
        if direccion is not None:
            updates.append("direccion = ?")
            params.append(direccion)
        if telefono is not None:
            updates.append("telefono = ?")
            params.append(telefono)
        if email is not None:
            updates.append("email = ?")
            params.append(email)

        if not updates:
            print("No hay datos para actualizar en el cliente.")
            return False

        query = f"UPDATE clientes SET {', '.join(updates)} WHERE cliente_id = ?"
        params.append(cliente_id)

        try:
            self.session.execute(query, params)
            print(f"Cliente '{cliente_id}' actualizado correctamente.")
            return True
        except Exception as e:
            print(f"Error al actualizar cliente: {e}")
            return False

    def borrar_cliente(self, cliente_id):
        query = "DELETE FROM clientes WHERE cliente_id = ?"
        try:
            self.session.execute(query, (cliente_id,))
            print(f"Cliente '{cliente_id}' borrado correctamente.")
            return True
        except Exception as e:
            print(f"Error al borrar cliente: {e}")
            return False

    # --- Operaciones para la tabla 'guias_envio_por_id' ---
    def insertar_guia_envio_por_id(self, guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados):
        query = """
        INSERT INTO guias_envio_por_id (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
        VALUES ((UUID)?, (UUID)?, (UUID)?, (UUID)?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        try:
            self.session.execute(query, (guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados))
            print(f"Guía de envío '{guia_id}' insertada/actualizada correctamente en guias_envio_por_id.")
            return True
        except Exception as e:
            print(f"Error al insertar/actualizar guía de envío por ID: {e}")
            return False

    def seleccionar_guia_envio_por_id(self, guia_id):
        query = "SELECT * FROM guias_envio_por_id WHERE guia_id = (UUID)%s"%(guia_id)
        try:
            row = self.session.execute(query).one()
            return row
        except Exception as e:
            print(f"Error al seleccionar guía de envío por ID: {e}")
            return None

    def actualizar_guia_envio_por_id(self, guia_id, cliente_id=None, sucursal_origen_id=None, sucursal_destino_id=None, fecha_venta=None, hora_venta=None, estado_envio=None, peso_kg=None, volumen_m3=None, valor_declarado=None, direccion_destino=None, coordenadas_destino=None, articulos_enviados=None):
        updates = []
        params = []
        if cliente_id is not None:
            updates.append("cliente_id = ?")
            params.append(cliente_id)
        if sucursal_origen_id is not None:
            updates.append("sucursal_origen_id = ?")
            params.append(sucursal_origen_id)
        if sucursal_destino_id is not None:
            updates.append("sucursal_destino_id = ?")
            params.append(sucursal_destino_id)
        if fecha_venta is not None:
            updates.append("fecha_venta = ?")
            params.append(fecha_venta)
        if hora_venta is not None:
            updates.append("hora_venta = ?")
            params.append(hora_venta)
        if estado_envio is not None:
            updates.append("estado_envio = ?")
            params.append(estado_envio)
        if peso_kg is not None:
            updates.append("peso_kg = ?")
            params.append(peso_kg)
        if volumen_m3 is not None:
            updates.append("volumen_m3 = ?")
            params.append(volumen_m3)
        if valor_declarado is not None:
            updates.append("valor_declarado = ?")
            params.append(valor_declarado)
        if direccion_destino is not None:
            updates.append("direccion_destino = ?")
            params.append(direccion_destino)
        if coordenadas_destino is not None:
            updates.append("coordenadas_destino = ?")
            params.append(coordenadas_destino)
        if articulos_enviados is not None:
            updates.append("articulos_enviados = ?")
            params.append(articulos_enviados)

        if not updates:
            print("No hay datos para actualizar en la guía de envío por ID.")
            return False

        query = f"UPDATE guias_envio_por_id SET {', '.join(updates)} WHERE guia_id = ?"
        params.append(guia_id)

        try:
            self.session.execute(query, params)
            print(f"Guía de envío '{guia_id}' actualizada correctamente en guias_envio_por_id.")
            return True
        except Exception as e:
            print(f"Error al actualizar guía de envío por ID: {e}")
            return False

    def borrar_guia_envio_por_id(self, guia_id):
        query = "DELETE FROM guias_envio_por_id WHERE guia_id = ?"
        try:
            self.session.execute(query, (guia_id,))
            print(f"Guía de envío '{guia_id}' borrada de guias_envio_por_id correctamente.")
            return True
        except Exception as e:
            print(f"Error al borrar guía de envío por ID: {e}")
            return False

    # --- Operaciones para la tabla 'guias_envio_por_sucursal_fecha' ---
    # Nota: Las operaciones de esta tabla deben ser consistentes con 'guias_envio_por_id'
    # para mantener la redundancia de datos.
    def insertar_guia_envio_por_sucursal_fecha(self, sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados):
        query = """
        INSERT INTO guias_envio_por_sucursal_fecha (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
        VALUES ((UUID)?, ?, (UUID)?, (UUID)?, (UUID)?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        try:
            self.session.execute(query, (sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados))
            print(f"Guía de envío '{guia_id}' insertada/actualizada correctamente en guias_envio_por_sucursal_fecha.")
            return True
        except Exception as e:
            print(f"Error al insertar/actualizar guía de envío por sucursal y fecha: {e}")
            return False

    def seleccionar_guias_envio_por_sucursal_fecha(self, sucursal_origen_id, fecha_venta):
        query = "SELECT * FROM guias_envio_por_sucursal_fecha WHERE sucursal_origen_id = ((UUID)%s) AND (fecha_venta = toDate('%s'))"%(sucursal_origen_id, fecha_venta)
        try:
            rows = self.session.execute(query)
            return rows
        except Exception as e:
            print(f"Error al seleccionar guías de envío por sucursal y fecha: {e}")
            return None

    def seleccionar_guia_especifica_por_sucursal_fecha(self, sucursal_origen_id, fecha_venta, guia_id):
        query = "SELECT * FROM guias_envio_por_sucursal_fecha WHERE sucursal_origen_id = ? AND fecha_venta = ? AND guia_id = ?"
        try:
            row = self.session.execute(query, (sucursal_origen_id, fecha_venta, guia_id)).one()
            return row
        except Exception as e:
            print(f"Error al seleccionar guía específica por sucursal y fecha: {e}")
            return None

    def actualizar_guia_envio_por_sucursal_fecha(self, sucursal_origen_id, fecha_venta, guia_id, cliente_id=None, sucursal_destino_id=None, hora_venta=None, estado_envio=None, peso_kg=None, volumen_m3=None, valor_declarado=None, direccion_destino=None, coordenadas_destino=None, articulos_enviados=None):
        updates = []
        params = []
        if cliente_id is not None:
            updates.append("cliente_id = ?")
            params.append(cliente_id)
        if sucursal_destino_id is not None:
            updates.append("sucursal_destino_id = ?")
            params.append(sucursal_destino_id)
        if hora_venta is not None:
            updates.append("hora_venta = ?")
            params.append(hora_venta)
        if estado_envio is not None:
            updates.append("estado_envio = ?")
            params.append(estado_envio)
        if peso_kg is not None:
            updates.append("peso_kg = ?")
            params.append(peso_kg)
        if volumen_m3 is not None:
            updates.append("volumen_m3 = ?")
            params.append(volumen_m3)
        if valor_declarado is not None:
            updates.append("valor_declarado = ?")
            params.append(valor_declarado)
        if direccion_destino is not None:
            updates.append("direccion_destino = ?")
            params.append(direccion_destino)
        if coordenadas_destino is not None:
            updates.append("coordenadas_destino = ?")
            params.append(coordenadas_destino)
        if articulos_enviados is not None:
            updates.append("articulos_enviados = ?")
            params.append(articulos_enviados)

        if not updates:
            print("No hay datos para actualizar en la guía de envío por sucursal y fecha.")
            return False

        query = f"UPDATE guias_envio_por_sucursal_fecha SET {', '.join(updates)} WHERE sucursal_origen_id = ? AND fecha_venta = ? AND guia_id = ?"
        params.extend([sucursal_origen_id, fecha_venta, guia_id])

        try:
            self.session.execute(query, params)
            print(f"Guía de envío '{guia_id}' actualizada correctamente en guias_envio_por_sucursal_fecha.")
            return True
        except Exception as e:
            print(f"Error al actualizar guía de envío por sucursal y fecha: {e}")
            return False

    def borrar_guia_envio_por_sucursal_fecha(self, sucursal_origen_id, fecha_venta, guia_id):
        query = "DELETE FROM guias_envio_por_sucursal_fecha WHERE sucursal_origen_id = ? AND fecha_venta = ? AND guia_id = ?"
        try:
            self.session.execute(query, (sucursal_origen_id, fecha_venta, guia_id))
            print(f"Guía de envío '{guia_id}' borrada de guias_envio_por_sucursal_fecha correctamente.")
            return True
        except Exception as e:
            print(f"Error al borrar guía de envío por sucursal y fecha: {e}")
            return False

    def ejecutar_batch(self, queries_with_params):
        """
        Ejecuta un conjunto de queries en un batch.
        queries_with_params: Lista de tuplas, donde cada tupla es (query_string, params_tuple).
        """
        batch = BatchStatement()
        try:
            for query_string, params_tuple in queries_with_params:
                batch.add(SimpleStatement(query_string), params_tuple)
            self.session.execute(batch)
            print("Batch ejecutado correctamente.")
            return True
        except Exception as e:
            print(f"Error al ejecutar batch: {e}")
            return False

if __name__ == "__main__":
    # Ejemplo de uso de operaciones.py
    # Asegúrate de que tu base de datos Cassandra esté corriendo
    # y que el keyspace 'inventario_logistica' y las tablas existan (puedes crearlos con schema.cql)

    # Puedes especificar los puntos de contacto, puerto, keyspace y credenciales
    # Si Cassandra está en tu máquina local sin autenticación, usa los valores por defecto
    cass_ops = CassandraOperations(contact_points=['127.0.0.1'], keyspace='inventario_logistica')
    # Si usas autenticación:
    # cass_ops = CassandraOperations(contact_points=['your_cassandra_ip'], username='your_user', password='your_password', keyspace='inventario_logistica')

    try:
        # --- Ejemplos de uso de Sucursales ---
        print("\n--- Operaciones con Sucursales ---")
        new_sucursal_id = uuid.UUID('d1a1b1c1-1001-4111-8111-000000000005')
        cass_ops.insertar_sucursal(new_sucursal_id, 'Sucursal Pruebas', '192.168.1.200', 'Quito')
        sucursal = cass_ops.seleccionar_sucursal_por_id(new_sucursal_id)
        if sucursal:
            print(f"Sucursal Seleccionada: {sucursal.nombre_sucursal}, {sucursal.ciudad}")
        cass_ops.actualizar_sucursal(new_sucursal_id, ciudad='Guayaquil')
        sucursal = cass_ops.seleccionar_sucursal_por_id(new_sucursal_id)
        if sucursal:
            print(f"Sucursal Actualizada: {sucursal.nombre_sucursal}, {sucursal.ciudad}")

        todas_sucursales = cass_ops.seleccionar_todas_sucursales()
        if todas_sucursales:
            print("Todas las Sucursales:")
            for s in todas_sucursales:
                print(f"- {s.nombre_sucursal} ({s.ciudad})")
        
        # Ojo: No se recomienda borrar datos de prueba para mantener la consistencia con test_data.cql
        # cass_ops.borrar_sucursal(new_sucursal_id)

        # --- Ejemplos de uso de Artículos por Sucursal ---
        print("\n--- Operaciones con Artículos por Sucursal ---")
        sucursal_alpha_id = uuid.UUID('d1a1b1c1-1001-4111-8111-000000000001') # ID de Sucursal Alpha del test_data.cql
        new_articulo_id = uuid.UUID('1a1a1a1a-1111-4111-8111-000000000041')
        cass_ops.insertar_articulo_por_sucursal(sucursal_alpha_id, new_articulo_id, 'Mouse Ergonómico', 'Mouse para uso prolongado', 100, 'unidades', 0.2)
        articulo = cass_ops.seleccionar_articulo_especifico_por_sucursal(sucursal_alpha_id, new_articulo_id)
        if articulo:
            print(f"Artículo Seleccionado: {articulo.nombre}, Cantidad: {articulo.cantidad}")
        cass_ops.actualizar_articulo_por_sucursal(sucursal_alpha_id, new_articulo_id, cantidad=90)
        articulo = cass_ops.seleccionar_articulo_especifico_por_sucursal(sucursal_alpha_id, new_articulo_id)
        if articulo:
            print(f"Artículo Actualizado: {articulo.nombre}, Cantidad: {articulo.cantidad}")
        
        articulos_alpha = cass_ops.seleccionar_articulos_por_sucursal(sucursal_alpha_id)
        if articulos_alpha:
            print(f"Artículos en Sucursal Alpha ({sucursal_alpha_id}):")
            for a in articulos_alpha:
                print(f"- {a.nombre}: {a.cantidad} {a.unidad_medida}")
        
        # cass_ops.borrar_articulo_por_sucursal(sucursal_alpha_id, new_articulo_id)


        # --- Ejemplos de uso de Clientes ---
        print("\n--- Operaciones con Clientes ---")
        new_cliente_id = uuid.UUID('c1b2c3d4-e5f6-7890-1234-567890abcd16')
        cass_ops.insertar_cliente(new_cliente_id, 'Roberto', 'Perez', 'Calle Falsa 123, Springfield', '5598765432', 'roberto.perez@email.com')
        cliente = cass_ops.seleccionar_cliente_por_id(new_cliente_id)
        if cliente:
            print(f"Cliente Seleccionado: {cliente.nombre} {cliente.apellido}, Email: {cliente.email}")
        cass_ops.actualizar_cliente(new_cliente_id, telefono='5512345678')
        cliente = cass_ops.seleccionar_cliente_por_id(new_cliente_id)
        if cliente:
            print(f"Cliente Actualizado: {cliente.nombre} {cliente.apellido}, Teléfono: {cliente.telefono}")

        todos_clientes = cass_ops.seleccionar_todos_clientes()
        if todos_clientes:
            print("Todos los Clientes:")
            for c in todos_clientes:
                print(f"- {c.nombre} {c.apellido}")
        
        # cass_ops.borrar_cliente(new_cliente_id)

        # --- Ejemplos de uso de Guías de Envío ---
        print("\n--- Operaciones con Guías de Envío ---")
        guia_test_id = uuid.UUID('f0e1d2c3-b4a5-4678-9012-34567890abcd')
        cliente_ana_id = uuid.UUID('c1b2c3d4-e5f6-7890-1234-567890abcd01')
        sucursal_beta_id = uuid.UUID('d1a1b1c1-1001-4111-8111-000000000002')
        sucursal_gamma_id = uuid.UUID('d1a1b1c1-1001-4111-8111-000000000003')
        
        fecha_test = datetime.date(2025, 6, 1)
        hora_test = datetime.time(14, 30, 0)
        articulos_test = {'Laptop Gamer Pro': 1, 'Mouse Inalámbrico Pro': 1}

        # Insertar en ambas tablas (simulando una venta)
        cass_ops.insertar_guia_envio_por_id(
            guia_test_id, cliente_ana_id, sucursal_beta_id, sucursal_gamma_id,
            fecha_test, hora_test, 'EN_TRANSITO', 3.5, 0.02, 1600.00,
            'Calle Falsa 123, Destino', '19.000,-99.000', articulos_test
        )
        cass_ops.insertar_guia_envio_por_sucursal_fecha(
            sucursal_beta_id, fecha_test, guia_test_id, cliente_ana_id,
            sucursal_gamma_id, hora_test, 'EN_TRANSITO', 3.5, 0.02, 1600.00,
            'Calle Falsa 123, Destino', '19.000,-99.000', articulos_test
        )

        guia_id_seleccionada = cass_ops.seleccionar_guia_envio_por_id(guia_test_id)
        if guia_id_seleccionada:
            print(f"Guía por ID seleccionada: {guia_id_seleccionada.guia_id}, Estado: {guia_id_seleccionada.estado_envio}")

        guias_por_fecha_sucursal = cass_ops.seleccionar_guias_envio_por_sucursal_fecha(sucursal_beta_id, fecha_test)
        if guias_por_fecha_sucursal:
            print(f"Guías para Sucursal Beta en {fecha_test}:")
            for g in guias_por_fecha_sucursal:
                print(f"- Guía ID: {g.guia_id}, Estado: {g.estado_envio}")
        
        cass_ops.actualizar_guia_envio_por_id(guia_test_id, estado_envio='ENTREGADO')
        cass_ops.actualizar_guia_envio_por_sucursal_fecha(sucursal_beta_id, fecha_test, guia_test_id, estado_envio='ENTREGADO')
        guia_id_seleccionada = cass_ops.seleccionar_guia_envio_por_id(guia_test_id)
        if guia_id_seleccionada:
            print(f"Guía por ID actualizada: {guia_id_seleccionada.guia_id}, Nuevo Estado: {guia_id_seleccionada.estado_envio}")

        # Ejemplo de Batch (actualizar el estado de la guía de envío en ambas tablas)
        print("\n--- Ejemplo de Operación Batch ---")
        guia_update_id = uuid.UUID('11111111-1111-4111-8111-111111111111') # Una de las guías de test_data.cql
        sucursal_origen_update = uuid.UUID('d1a1b1c1-1001-4111-8111-000000000001') # Sucursal Alpha
        fecha_update = datetime.date(2025, 5, 20) # Fecha de esa guía

        queries_to_batch = [
            ("UPDATE guias_envio_por_id SET estado_envio = ? WHERE guia_id = ?", ('CANCELADO', guia_update_id)),
            ("UPDATE guias_envio_por_sucursal_fecha SET estado_envio = ? WHERE sucursal_origen_id = ? AND fecha_venta = ? AND guia_id = ?", ('CANCELADO', sucursal_origen_update, fecha_update, guia_update_id))
        ]
        cass_ops.ejecutar_batch(queries_to_batch)
        guia_post_batch = cass_ops.seleccionar_guia_envio_por_id(guia_update_id)
        if guia_post_batch:
            print(f"Guía después de batch: {guia_post_batch.guia_id}, Estado: {guia_post_batch.estado_envio}")


    finally:
        cass_ops.close()
