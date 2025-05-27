# gestion_inventario.py
from operaciones import CassandraOperations
import uuid
import datetime

class GestionInventario:
    def __init__(self, contact_points=['192.168.1.101'], port=9042, keyspace='inventario_logistica', username=None, password=None):
        self.db_ops = CassandraOperations(contact_points, port, keyspace, username, password)

    def __del__(self):
        self.db_ops.close()

    # --- Funcionalidades del Inventario y Logística ---

    def consultar_inventario_local(self, sucursal_id):
        """
        Consulta y muestra el inventario de artículos en una sucursal específica.
        """
        print(f"\n--- Consultando Inventario Local en Sucursal ID: {sucursal_id} ---")
        sucursal = self.db_ops.seleccionar_sucursal_por_id(sucursal_id)
        if not sucursal:
            print(f"Error: No se encontró la sucursal con ID {sucursal_id}.")
            return

        print(f"Inventario para la Sucursal: {sucursal.nombre_sucursal} ({sucursal.ciudad})")
        articulos = self.db_ops.seleccionar_articulos_por_sucursal(sucursal_id)
        if articulos:
            if not articulos.current_rows:
                print("No hay artículos en el inventario de esta sucursal.")
                return

            for articulo in articulos:
                print(f"  - ID: {articulo.articulo_id}")
                print(f"    Nombre: {articulo.nombre}")
                print(f"    Descripción: {articulo.descripcion}")
                print(f"    Cantidad: {articulo.cantidad} {articulo.unidad_medida}")
                print(f"    Capacidad por unidad: {articulo.capacidad_almacenamiento} m3")
                print("-" * 30)
        else:
            print("Error al recuperar los artículos.")

    def consultar_inventario_distribuido(self):
        """
        Consulta y muestra el inventario de artículos en todas las sucursales.
        """
        print("\n--- Consultando Inventario Distribuido (Todas las Sucursales) ---")
        sucursales = self.db_ops.seleccionar_todas_sucursales()
        if not sucursales or not sucursales.current_rows:
            print("No se encontraron sucursales en la base de datos.")
            return

        for sucursal in sucursales:
            print(f"\nInventario para la Sucursal: {sucursal.nombre_sucursal} (ID: {sucursal.sucursal_id}, Ciudad: {sucursal.ciudad})")
            print("-" * 20)
            articulos = self.db_ops.seleccionar_articulos_por_sucursal(sucursal.sucursal_id)
            if articulos and articulos.current_rows:
                for articulo in articulos:
                    print(f"  - ID: {articulo.articulo_id}")
                    print(f"    Nombre: {articulo.nombre}")
                    print(f"    Cantidad: {articulo.cantidad} {articulo.unidad_medida}")
                    print("-" * 20)
            else:
                print("  No hay artículos en el inventario de esta sucursal.")

    def agregar_articulo_a_inventario_distribuido(self, lista_sucursales, nombre_articulo, descripcion, cantidad, unidad_medida, capacidad_almacenamiento):
        """
        Agrega un artículo en el inventario de una sucursal específica.
        Genera un nuevo UUID para el artículo si no se especifica uno.
        """
        print(f"\n--- Agregando/Actualizando Artículo en Inventario Distribuido ---")

        # Generar un nuevo UUID para el artículo
        articulo_id = uuid.uuid4()
        for s in self.db_ops.seleccionar_todas_sucursales():
            lista_sucursales.append(s.sucursal_id)
        cantidades = self.dividir_en_n_montones_equitativos(cantidad, len(lista_sucursales))

        for i_sucursal in range(len(lista_sucursales)):
            sucursal = self.db_ops.seleccionar_sucursal_por_id(lista_sucursales[i_sucursal])
            if not sucursal:
                print(f"Error: La sucursal con ID {sucursal_id} no existe. No se puede agregar el artículo.")
                return False

            if self.db_ops.insertar_articulo_por_sucursal(
                lista_sucursales[i_sucursal], articulo_id, nombre_articulo, descripcion, cantidades[i_sucursal], unidad_medida, capacidad_almacenamiento
            ):
                print(f"Artículo '{nombre_articulo}' (ID: {articulo_id}) agregado/actualizado en la sucursal '{sucursal.nombre_sucursal}'.")
                #return True
            else:
                print(f"Fallo al agregar/actualizar el artículo '{nombre_articulo}'.")
                return False
    
    def dividir_en_n_montones_equitativos(self, numero, n_montones):
        if not isinstance(numero, int) or numero < 0:
            raise ValueError("El número a dividir debe ser un entero no negativo.")
        
        if not isinstance(n_montones, int) or n_montones <= 0:
            raise ValueError("El número de montones debe ser un entero positivo.")

        # Caso especial: si el número es 0, todos los montones son 0
        if numero == 0:
            return [0] * n_montones
        
        # Caso especial: si solo hay 1 montón, es el número completo
        if n_montones == 1:
            return [numero]

        cociente = numero // n_montones  # División entera
        residuo = numero % n_montones    # El resto de la división

        montones = [cociente] * n_montones  # Inicializa todos los montones con el cociente

        # Distribuir el residuo
        # Los primeros 'residuo' montones recibirán un +1
        for i in range(residuo):
            montones[i] += 1
        
        return montones

    def consultar_sucursales(self):
        """
        Consulta y muestra la lista de todas las sucursales.
        """
        print("\n--- Consultando Sucursales ---")
        sucursales = self.db_ops.seleccionar_todas_sucursales()
        if sucursales and sucursales.current_rows:
            print("Lista de Sucursales:")
            for s in sucursales:
                print(f"  - ID: {s.sucursal_id}")
                print(f"    Nombre: {s.nombre_sucursal}")
                print(f"    Ciudad: {s.ciudad}")
                print(f"    Dirección IP: {s.direccion_ip}")
                print("-" * 30)
        else:
            print("No se encontraron sucursales.")
    
    def obtener_sucursal_id(self, ip):
        """
        Obtiene el id a partir una dirección ip
        """
        sucursal_id = self.db_ops.obtener_sucursal_id_por_ip(ip)
        if (id != None):
            return str(sucursal_id)
        else:
            print("No se encontraron sucursales.")
            return None

    def agregar_sucursal(self, nombre_sucursal, direccion_ip, ciudad):
        """
        Agrega una nueva sucursal a la base de datos.
        Genera un nuevo UUID para la sucursal.
        """
        print(f"\n--- Agregando Nueva Sucursal ---")
        new_sucursal_id = uuid.uuid4()
        if self.db_ops.insertar_sucursal(new_sucursal_id, nombre_sucursal, direccion_ip, ciudad):
            print(f"Sucursal '{nombre_sucursal}' (ID: {new_sucursal_id}) agregada correctamente.")
            return True
        else:
            print(f"Fallo al agregar la sucursal '{nombre_sucursal}'.")
            return False

    def consultar_lista_clientes(self):
        """
        Consulta y muestra la lista de todos los clientes.
        """
        print("\n--- Consultando Lista de Clientes ---")
        print("-" * 30)
        clientes = self.db_ops.seleccionar_todos_clientes()
        if clientes and clientes.current_rows:
            print("Lista de Clientes:")
            for c in clientes:
                print(f"  - ID: {c.cliente_id}")
                print(f"    Nombre: {c.nombre} {c.apellido}")
                print(f"    Email: {c.email}")
                print(f"    Teléfono: {c.telefono}")
                print("-" * 30)
        else:
            print("No se encontraron clientes.")

    def agregar_actualizar_cliente(self, cliente_id=None, nombre=None, apellido=None, direccion=None, telefono=None, email=None):
        """
        Agrega un nuevo cliente o actualiza uno existente.
        Si cliente_id es None, se crea un nuevo cliente.
        Si cliente_id es proporcionado, se intenta actualizar.
        """
        if cliente_id is None:
            cliente_id = uuid.uuid4()
            print(f"\n--- Agregando Nuevo Cliente (ID generado: {cliente_id}) ---")
            if self.db_ops.insertar_cliente(cliente_id, nombre, apellido, direccion, telefono, email):
                print(f"Cliente '{nombre} {apellido}' agregado correctamente.")
                return cliente_id
            else:
                print(f"Fallo al agregar el cliente '{nombre} {apellido}'.")
                return None
        else:
            print(f"\n--- Actualizando Cliente (ID: {cliente_id}) ---")
            cliente_existente = self.db_ops.seleccionar_cliente_por_id(cliente_id)
            if not cliente_existente:
                print(f"Error: No se encontró el cliente con ID {cliente_id} para actualizar.")
                return None
            
            # Usar los valores existentes si los nuevos son None
            nombre = nombre if nombre is not None else cliente_existente.nombre
            apellido = apellido if apellido is not None else cliente_existente.apellido
            direccion = direccion if direccion is not None else cliente_existente.direccion
            telefono = telefono if telefono is not None else cliente_existente.telefono
            email = email if email is not None else cliente_existente.email

            if self.db_ops.actualizar_cliente(cliente_id, nombre, apellido, direccion, telefono, email):
                print(f"Cliente '{nombre} {apellido}' actualizado correctamente.")
                return cliente_id
            else:
                print(f"Fallo al actualizar el cliente '{cliente_id}'.")
                return None

    def ver_guias_envio_generadas(self, sucursal_origen_id=None, fecha=None, guia_id=None):
        """
        Permite ver guías de envío:
        - Todas (si no se pasan parámetros)
        - Por sucursal y fecha (si se pasan sucursal_origen_id y fecha)
        - Una guía específica por ID (si se pasa solo guia_id)
        """
        print("\n--- Consultando Guías de Envío ---")
        
        if guia_id:
            guia = self.db_ops.seleccionar_guia_envio_por_id(guia_id)
            if guia:
                print(f"Detalle de la Guía de Envío ID: {guia.guia_id}")
                print(f"  Cliente ID: {guia.cliente_id if guia.cliente_id else 'N/A (Movimiento entre sucursales)'}")
                print(f"  Sucursal Origen: {guia.sucursal_origen_id}")
                print(f"  Sucursal Destino: {guia.sucursal_destino_id}")
                print(f"  Fecha Venta: {guia.fecha_venta}, Hora Venta: {guia.hora_venta}")
                print(f"  Estado: {guia.estado_envio}")
                print(f"  Peso: {guia.peso_kg} kg, Volumen: {guia.volumen_m3} m3")
                print(f"  Valor Declarado: ${guia.valor_declarado:.2f}")
                print(f"  Dirección Destino: {guia.direccion_destino}")
                print(f"  Artículo ID: {guia.articulos_enviados}")
                print("-" * 40)
            else:
                print(f"No se encontró la guía de envío con ID: {guia_id}.")
        elif sucursal_origen_id and fecha:
            print(f"Guías de envío para Sucursal Origen ID: {sucursal_origen_id} en la fecha: {fecha}")
            guias = self.db_ops.seleccionar_guias_envio_por_sucursal_fecha(sucursal_origen_id, fecha)
            if guias and guias.current_rows:
                for guia in guias:
                    print(f"  - Guía ID: {guia.guia_id}")
                    print(f"    Cliente ID: {guia.cliente_id if guia.cliente_id else 'N/A'}")
                    print(f"    Sucursal Destino: {guia.sucursal_destino_id}")
                    print(f"    Estado: {guia.estado_envio}")
                    print(f"    Artículo ID: {guia.articulos_enviados}")
                    print("-" * 30)
            else:
                print(f"No se encontraron guías para la sucursal {sucursal_origen_id} en la fecha {fecha}.")
        else:
            print("Consultando todas las guías de envío (puede ser lento en grandes volúmenes):")
            # Esto puede ser ineficiente sin un índice secundario o tabla dedicada si la base de datos es muy grande.
            # Para propósitos de este ejercicio, se hará una consulta simple.
            # En un entorno de producción, se buscaría una tabla con una PK adecuada para este tipo de consulta.
            # Para este esquema, se tendría que escanear la tabla guias_envio_por_id completa.
            # No hay una función 'seleccionar_todas_guias_envio' para guias_envio_por_id directamente aquí
            # para evitar una query con alto costo de scan en una tabla con PK que no está diseñada para ello.
            # Por simplicidad, se mostrará un mensaje para este caso.
            print("  Para ver todas las guías, necesitarías una consulta que escanee guias_envio_por_id,")
            print("  lo cual no es óptimo para Cassandra. Por favor, especifica una sucursal y fecha, o un ID de guía.")
            guias = self.db_ops.seleccionar_todas_guias_envio()
            if guias and guias.current_rows:
                for guia in guias:
                    print(f"  - Guía ID: {guia.guia_id}")
                    print(f"    Cliente ID: {guia.cliente_id if guia.cliente_id else 'N/A'}")
                    print(f"    Sucursal Destino: {guia.sucursal_destino_id}")
                    print(f"    Estado: {guia.estado_envio}")
                    print(f"    Artículo ID: {guia.articulos_enviados}")
                    print("-" * 30)
            else:
                print(f"No se encontraron guías para la sucursal {sucursal_origen_id} en la fecha {fecha}.")

    def enviar_entre_sucursales(cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados):
        now = datetime.now()
        guia_id = uuid.uuid4()
        cliente_id
        sucursal_origen_id
        sucursal_destino_id
        fecha_venta = now.strftime("%Y-%m-%d")
        hora_venta =now.strftime("%H:%M:%S")
        estado_envio = "Pendiente"
        peso_kg
        volumen_m3
        valor_declarado
        direccion_destino
        coordenadas_destino
        articulos_enviados

        self.db_ops.insertar_guia_envio_por_id(guia_id, cliente_id, sucursal_origen_id, sucursal_destino_id, fecha_venta, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
        self.db_ops.insertar_guia_envio_por_sucursal_fecha(sucursal_origen_id, fecha_venta, guia_id, cliente_id, sucursal_destino_id, hora_venta, estado_envio, peso_kg, volumen_m3, valor_declarado, direccion_destino, coordenadas_destino, articulos_enviados)
#AQUI
    def verificar_stock_local(self, sucursal_id, articulo_id):
        try:
            rows = self.db_ops.seleccionar_articulo_especifico_por_sucursal(sucursal_id, articulo_id)
            return rows.cantidad if rows else None
        except Exception as e:
            print(f"Error al verificar stock: {str(e)}")
            return None

    def actualizar_stock(self, sucursal_id, articulo_id, cantidad):
        new_cantidad = self.verificar_stock_local(sucursal_id, articulo_id) + cantidad
        try:
            self.db_ops.actualizar_articulo_por_sucursal(sucursal_id, articulo_id, cantidad=new_cantidad)
            return True
        except Exception as e:
            print(f"Error al actualizar stock: {str(e)}")
            return False
#Aqui DEM
    def obtener_ips_nodos_efimeros(self):
        ip_maestro = self.obtener_ip_maestro()  # Obtener IP del maestro
        if not ip_maestro:
            print("Maestro no encontrado")
            return []

        zk = KazooClient(hosts=ip_maestro + ':2181')  # Usar IP del maestro 👈
        zk.start()
        
        try:
            nodos = zk.get_children("/nodos_efimeros_cassandra")
            return [zk.get(f"/nodos_efimeros_cassandra/{nodo}")[0].decode() for nodo in nodos]
        finally:
            zk.stop()
    def obtener_ip_maestro(self):
        zk = KazooClient(hosts='127.0.0.1:2181')  # Conexión local inicial, aca puede ir la de 192.168....
        zk.start()
        
        try:
            # Ver quién es el líder actual
            leader = zk.get("/eleccion_maestro_cassandra/leader")[0].decode()
            return leader  # Devuelve la IP del maestro
        except:
            return None
        finally:
            zk.stop()

    def verificar_maestro_activo(self):
        maestro_ip = self.obtener_ip_maestro()
        if not maestro_ip:
            return False
            try:
                with socket.create_connection((maestro_ip.split(':')[0], 2181), timeout=2):
                    return True
            except:
                return False


    def monitorear_nodos(self):

            """Escucha cambios en los nodos y reelegir maestro si es necesario"""

            zk = KazooClient(hosts='127.0.0.1:2181')

            zk.start()



            zk.DataWatch("/eleccion_maestro_cassandra/leader")

            def vigilar_maestro(data, stat):

                if not data:  # Si no hay maestro

                    print("\n¡No hay maestro! Iniciando nueva elección...")

                    self.iniciar_eleccion_maestro()  # Función que ya tienes



            # Mantener el listener activo

            while True:

                time.sleep(5)
#NUEVAS
    def obtener_ip_maestro(self):
        """Obtiene la IP del nodo maestro desde ZooKeeper"""
        zk = KazooClient(hosts='127.0.0.1:2181')  # Conexión local
        zk.start()
        try:
            leader = zk.get("/eleccion_maestro_cassandra/leader")[0].decode()
            return leader
        except:
            return None
        finally:
            zk.stop()

    def verificar_maestro_activo(self):
        """Verifica si el nodo maestro está respondiendo"""
        maestro_ip = self.obtener_ip_maestro()  # <-- ¡ESPACIO NORMAL aquí!
        if not maestro_ip:
            return False
        try:
            with socket.create_connection((maestro_ip.split(':')[0], 2181), timeout=2):
                return True
        except:
            return False

    def monitorear_nodos(self):
        """Escucha cambios en los nodos"""
        zk = KazooClient(hosts='127.0.0.1:2181')
        zk.start()
        def vigilar_maestro(data, stat):
            if not data:
                print("\n¡No hay maestro! Iniciando nueva elección...")
        zk.DataWatch("/eleccion_maestro_cassandra/leader", vigilar_maestro)
        while True:
            time.sleep(5)

if __name__ == "__main__":
    # Asegúrate de que Cassandra esté corriendo y que hayas ejecutado schema.cql y test_data.cql
    # antes de ejecutar este script.

    gestion = GestionInventario(contact_points=['192.168.1.101'], keyspace='inventario_logistica')
    # Si usas autenticación:
    # gestion = GestionInventario(contact_points=['your_cassandra_ip'], username='your_user', password='your_password', keyspace='inventario_logistica')

    try:
        # Ejemplo de UUIDs de test_data.cql para facilitar las pruebas
        SUCURSAL_ALPHA_ID = 'd1a1b1c1-1001-4111-8111-000000000001'
        SUCURSAL_BETA_ID = 'd1a1b1c1-1001-4111-8111-000000000002'
        SUCURSAL_GAMMA_ID = 'd1a1b1c1-1001-4111-8111-000000000003'
        SUCURSAL_DELTA_ID = 'd1a1b1c1-1001-4111-8111-000000000004'

        GUIA_TEST_ID_1 = '11111111-1111-4111-8111-111111111111' # Venta 1
        FECHA_GUIA_TEST_1 = datetime.date(2025, 5, 20)

        print("{"+gestion.obtener_sucursal_id('192.168.1.101')+"}")
        gestion.consultar_inventario_local(gestion.obtener_sucursal_id('192.168.1.101'))
        # Listos 1,2,3,4,6
        # faltantes 5,7

        # --- Pruebas de funcionalidades ---

        # 1. Consultar inventario local
        # gestion.consultar_inventario_local(SUCURSAL_ALPHA_ID)

        # 2. Consultar inventario distribuido
        # gestion.consultar_inventario_distribuido()

        # 3. Agregar artículo al inventario distribuido
        # gestion.agregar_articulo_a_inventario_distribuido(
        #     SUCURSAL_BETA_ID,
        #     'Termo Inteligente',
        #     'Termo con pantalla LED para temperatura',
        #     50,
        #     'unidades',
        #     0.005
        # )
        # gestion.consultar_inventario_local(SUCURSAL_BETA_ID) # Verificar la adición

        # 4. Consultar sucursales
        # gestion.consultar_sucursales()

        # 5. Agregar sucursales
        # gestion.agregar_sucursal('Sucursal Centro', '192.168.1.205', 'León')
        # gestion.consultar_sucursales() # Verificar la adición

        # 6. Consultar lista de clientes
        # gestion.consultar_lista_clientes()

        # 7. Agregar/Actualizar cliente
        # Agregar un nuevo cliente
        nuevo_cliente_id = gestion.agregar_actualizar_cliente(
            nombre='Nueva',
            apellido='Cliente',
            direccion='Calle de Prueba 1, Prueba',
            telefono='5500000000',
            email='nueva.cliente@example.com'
        )
        gestion.consultar_lista_clientes() # Verificar la adición

        # Actualizar un cliente existente (usando un ID de test_data.cql)
        cliente_actualizar_id = 'c1b2c3d4-e5f6-7890-1234-567890abcd01' # Ana Torres
        gestion.agregar_actualizar_cliente(
            cliente_id=cliente_actualizar_id,
            telefono='5599887766',
            email='ana.torres.new@email.com'
        )
        # Consultar el cliente actualizado para verificar
        # print("\n--- Cliente Actualizado (Ana Torres) ---")
        # ana_torres = gestion.db_ops.seleccionar_cliente_por_id(cliente_actualizar_id)
        # if ana_torres:
        #     print(f"ID: {ana_torres.cliente_id}, Nombre: {ana_torres.nombre} {ana_torres.apellido}, Teléfono: {ana_torres.telefono}, Email: {ana_torres.email}")

        # 8. Ver guías de envío generadas
        # Ver una guía específica
        # gestion.ver_guias_envio_generadas(guia_id=GUIA_TEST_ID_1)

        # Ver guías de una sucursal y fecha
        # gestion.ver_guias_envio_generadas(
        #     sucursal_origen_id=SUCURSAL_ALPHA_ID,
        #     fecha=FECHA_GUIA_TEST_1
        # )

        # Intenta ver todas las guías (mostrará el mensaje de advertencia)
        # gestion.ver_guias_envio_generadas()

    finally:
        print("\nCerrando conexiones...")
        # La conexión se cierra automáticamente al finalizar el script
        # gracias al método __del__ o puedes llamarlo explícitamente si prefieres
        # gestion.db_ops.close() # Descomentar si no confías en __del__ o para un control más preciso
