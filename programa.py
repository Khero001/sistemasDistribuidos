#Nuevos imports
from kazoo.client import KazooClient
from kazoo.recipe.election import Election  # Cambió LeaderElection -> Election
import socket
import time
import os
import socket
import threading
import uuid
import sys
import netifaces
from datetime import datetime
from gestion_inventario import GestionInventario

# --- Configuración Global ---
CONFIG_FILE = "config.txt"
MESSAGES = []
MY_ID = None
MY_IP = None
MY_PORT = None
gestion = None
sucursal_id = None
ALL_NODES_INFO = {}
#Nuevas variables zookeper
#ZOOKEEPER_HOSTS = '192.168.1.101:2181'
ZOOKEEPER_HOSTS = '192.168.1.101:2181,192.168.1.102:2181,192.168.1.97:2181,192.168.1.97:2181'  # ESTO SOLO ES DE PRUEBA, SE VA A CAMBIAR, REVISAR LINEAS 373 EN ADELANTE DE GESTION_INVENTARIO.py
ELECTION_PATH = "/eleccion_maestro_cassandra"

# --- NUEVO: Variables para nodo maestro ---
IS_MASTER = False
CONNECTED_NODES = {}  # {node_id: (ip, port)}
MASTER_CHECK_INTERVAL = 10  # segs, para hacer tareas periódicas si es maestro

# --- Funciones Utilitarias ---
def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

#nuevo getnode
def get_node_info():
    global MY_ID, MY_IP, MY_PORT, ALL_NODES_INFO, sucursal_id, gestion, IS_MASTER, CONNECTED_NODES
    try:
        print(f"\nBuscando configuración en {CONFIG_FILE}...")
        with open(CONFIG_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):  # Ignora líneas vacías y comentarios
                    parts = line.split(",")
                    if len(parts) == 3:
                        node_id, ip, port_str = parts
                        ALL_NODES_INFO[node_id] = (ip, int(port_str))
                        print(f"Registrado nodo: {node_id} - {ip}:{port_str}")

        print("\nBuscando IP local...")
        interfaces = netifaces.interfaces()
        for interface in interfaces:
            try:
                addrs = netifaces.ifaddresses(interface)
                if netifaces.AF_INET in addrs:
                    for ip_info in addrs[netifaces.AF_INET]:
                        current_ip = ip_info['addr']
                        print(f"Verificando IP: {current_ip} en interfaz {interface}")
                        for node_id, (node_ip, node_port) in ALL_NODES_INFO.items():
                            if node_ip == current_ip:
                                print(f"¡COINCIDENCIA! Soy el nodo {node_id}")
                                MY_ID = node_id
                                MY_IP = node_ip
                                MY_PORT = node_port
                                gestion = GestionInventario(contact_points=[MY_IP])
                                sucursal_id = gestion.obtener_sucursal_id(MY_IP)
                                IS_MASTER = False  # ZooKeeper decidirá
                                return MY_ID, MY_IP, MY_PORT, gestion, sucursal_id
            except Exception as e:
                print(f"Error revisando interfaz {interface}: {str(e)}")
                continue

        raise Exception(f"\n Ninguna IP local coincide con {CONFIG_FILE}. IPs encontradas: {[ip_info['addr'] for interface in netifaces.interfaces() for ip_info in netifaces.ifaddresses(interface).get(netifaces.AF_INET, [])]}")
    
    except FileNotFoundError:
        print(f"\n Archivo {CONFIG_FILE} no encontrado")
        sys.exit(1)
    except Exception as e:
        print(f"\n Error inesperado: {str(e)}")
        sys.exit(1)
# --- Funciones de Almacenamiento ---
def store_message(message):
    if MY_ID is None:
        return
    filename = f"mensajes_{MY_ID}.txt"
    try:
        if not os.path.exists(filename):
            open(filename, "w").close()
        with open(filename, "a", encoding="utf-8") as f:
            f.write(message + "\n")
            print(f"Mensaje almacenado en {filename}")
            f.close()
    except Exception as e:
        print(f"Error al guardar mensaje: {e}")

def show_messages():
    if MY_ID is None:
        return
    lineas = int(input("Ingrese el número de mensajes: "))
    filename = f"mensajes_{MY_ID}.txt"
    try:
        if not os.path.exists(filename):
            open(filename, "w").close()
        print("+"+("-"*20)+"+")
        os.system("tail -%d %s"%(lineas, filename))
        print("+"+("-"*20)+"+")
    except Exception as e:
        print(f"Error al guardar mensaje: {e}")

# --- Funciones de Red ---
def receive_messages(my_id, my_port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((MY_IP, my_port))
    server_socket.listen()
    print(f"Nodo {my_id} está escuchando en el puerto {my_port}...")
    while True:
        client_socket, addr = server_socket.accept()
        threading.Thread(target=handle_connection, args=(client_socket, my_id), daemon=True).start()
#NUEVAS DEF
#def nueva
def marcar_sucursal_inactiva(self, sucursal_id):
        """Marca una sucursal como caída en Cassandra"""
        query = """
        INSERT INTO estado_sucursales (
            sucursal_id,
            estado,
            ultima_actualizacion,
            es_master
        ) VALUES (%s, %s, %s, %s)
        """
        try:
            self.db_ops.session.execute(query, [
                uuid.UUID(sucursal_id),
                "INACTIVA",
                datetime.datetime.now(),
                False
            ])
            return True
        except Exception as e:
            print(f"Error al marcar sucursal como inactiva: {str(e)}")
            return False
#nueva
def obtener_sucursales_activas(self):
        """Devuelve una lista de IDs de sucursales activas"""
        query = "SELECT sucursal_id FROM estado_sucursales WHERE estado = %s"
        try:
            rows = self.db_ops.session.execute(query, ["ACTIVA"])
            return [str(row.sucursal_id) for row in rows]
        except Exception as e:
            print(f"Error al obtener sucursales activas: {str(e)}")
            return []
def handle_connection(client_socket, my_id):
    try:
        message_data = client_socket.recv(1024).decode('utf-8')
        if message_data:
            # --- PARTE 1: Mensajes normales de chat ---
            if "||" in message_data:
                sender_id, message_with_timestamp = message_data.split(":", 1)
                timestamp, message = message_with_timestamp.split("||", 1)
                full_message = f"[{get_timestamp()}] Nodo {sender_id}: {message}"
                print(full_message)
                store_message(f"[{timestamp}] Nodo {sender_id}: {message} (Recibido)")
                response = f"Recibido por Nodo {my_id} a las {get_timestamp()}"
            
            # --- PARTE 2: Mensajes para comprar artículos ---
            elif message_data.startswith("BLOQUEO_COMPRA:"):
                _, nodo_solicitante, articulo_id, cantidad = message_data.split(":")
                stock = gestion.verificar_stock_local(sucursal_id, articulo_id)
                
                if stock is not None and stock >= int(cantidad):
                    response = "APROBADO"
                else:
                    response = "DENEGADO"
            
            # --- PARTE NUEVA: Mensajes de nodos caídos (PARA SIMULAR FALLA) ---
            elif message_data.startswith("NODO_CAIDO:"):
                _, sucursal_fallida = message_data.split(":")
                print(f"\n¡ALERTA! La sucursal {sucursal_fallida} ha fallado")
                
                # Marcar como inactiva en Cassandra
                if gestion.marcar_sucursal_inactiva(sucursal_fallida):
                    response = "CAIDA_REGISTRADA"
                else:
                    response = "ERROR_AL_MARCAR_CAIDA"
            
            # --- PARTE 3: Lógica para el nodo maestro ---
            elif IS_MASTER and message_data.startswith("SOLICITUD_"):
                print(f"Nodo maestro recibió solicitud: {message_data}")
                response = "SOLICITUD_PROCESADA"
            
            # --- PARTE 4: Otros mensajes no reconocidos ---
            else:
                response = "MENSAJE_DESCONOCIDO"
            
            client_socket.sendall(response.encode('utf-8'))
    
    except Exception as e:
        print(f"Nodo {my_id}: Error al recibir mensaje: {e}")
    finally:
        client_socket.close()

def send_message(my_id, target_name, target_ip, target_port, message_text):
    try:
        if target_ip != MY_IP:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.settimeout(5)
                client_socket.connect((target_ip, target_port))
                timestamp = get_timestamp()
                full_message = f"{my_id}:{timestamp}||{message_text}"
                client_socket.sendall(full_message.encode('utf-8'))
                #print(f"[{timestamp}] Nodo {my_id}: {message_text} (Enviado)")
                store_message(f"[{timestamp}] Nodo {my_id}: {message_text} (Enviado)")
                response = client_socket.recv(1024).decode('utf-8')
                #print(f"Respuesta de {target_name}: {response}")
    except Exception as e:
        print(f"Nodo {my_id}: Error al enviar mensaje: {e}")

# --- NUEVO: Función que el nodo maestro ejecuta periódicamente ---
def maestro_periodico():
    while True:
        # Ejemplo simple: enviar "PING" a todos los nodos conectados para chequear salud
        for node_id, (ip, port) in CONNECTED_NODES.items():
            try:
                send_message(MY_ID, node_id, ip, port, "PING desde maestro")
            except Exception as e:
                print(f"Error enviando ping a {node_id}: {e}")
        threading.Event().wait(MASTER_CHECK_INTERVAL)

# --- Funciones del Sistema Distribuido ---
def consultar_inventario_local():
    gestion.consultar_inventario_local(sucursal_id)

def consultar_inventario_distribuido():
    if not IS_MASTER:
        print("Error: Operación solo para nodo maestro")
        return
    gestion.consultar_inventario_distribuido()

def agregar_articulo_distribuido():
    lista_sucursales = []
    nombre_articulo = input("Ingrese el nombre del artículo: ")
    descripcion = input("Ingrese la descripción del artículo: ")
    cantidad = int(input("Ingrese la cantidad de artículos: "))
    unidad_medida = input("Ingrese la unidad media de los artículos del artículo: ")
    capacidad_almacenamiento = input("Ingrese la capacidad de almacenamiento del artículo: ")

    gestion.agregar_articulo_a_inventario_distribuido(lista_sucursales, nombre_articulo, descripcion, cantidad, unidad_medida, capacidad_almacenamiento)

def consultar_clientes():
    gestion.consultar_lista_clientes()

def actualizar_cliente():
    print("Ingresar los datos que se piden a continuación.")
    print("Si se deja vacío algún campo se mantendrá igual.")
    print("En el caso del ID, ingresarlo actualizará el usuario")
    print("dejarlo vacío creará un nuevo cliente")

    cli = input("Ingresar el ID del cliente: ")
    name = input("Ingresar nombre del cliente: ")
    ap = input("Ingresar apellido del cliente: ")
    direc = input("Ingresar dirección del cliente: ")
    tel = input("Ingresar teléfono del cliente: ")
    mail = input("Ingresar correo electrónico del cliente: ")
    try:
        nuevo_cliente_id = gestion.agregar_actualizar_cliente(
            cliente_id= None if cli == "" else cli,
            nombre= None if name == "" else name,
            apellido= None if ap == "" else ap,
            direccion= None if direc == "" else direc,
            telefono= None if tel == "" else tel,
            email= None if mail == "" else mail
        )
    except ValueError:
        print("Datos ingresados no válidos.")

def comprar_articulo():
    try:
        print("\n=== COMPRAR ARTICULO ===")
        gestion.consultar_inventario_local(sucursal_id)
        
        articulo_id = input("ID del articulo a comprar: ").strip()
        cantidad = int(input("Cantidad a comprar: "))
        cliente_id = input("ID del cliente: ")
        
        print("\nVerificando con otras sucursales...")
        
        # Preguntar a otros nodos
        mensaje = f"BLOQUEO_COMPRA:{MY_ID}:{articulo_id}:{cantidad}"
        respuestas = {}
        
        for node_id, (ip, port) in ALL_NODES_INFO.items():
            if node_id != MY_ID:
                try:
                    send_message(MY_ID, node_id, ip, port, mensaje)
                    respuestas[node_id] = True
                except:
                    respuestas[node_id] = False
        
        if not all(respuestas.values()):
            print("Algunas sucursales no respondieron. Abortando compra.")
            return
        
        # Intentar comprar
        if gestion.actualizar_stock(sucursal_id, articulo_id, -1 * cantidad,):
            gestion.generar_guia(sucursal_id, articulo_id, cantidad, cliente_id = cliente_id)
            print("Compra exitosa. Stock actualizado.")
        else:
            print("No hay suficiente stock o el articulo no existe.")
            
    except ValueError:
        print("Error: Ingresa un numero valido para la cantidad.")
    except Exception as e:
        print(f"Error inesperado: {str(e)}")


def ver_guias_envio():
    print("[Funcionalidad en desarrollo] Ver guías de envío generadas")
    print("Opciones para ver guías:")
    print("  a. Ver una guía específica por ID")
    print("  b. Ver guías por sucursal y fecha")
    print("  c. Ver todas las guías (¡Advertencia: puede ser lento!)")
    sub_opcion = input("Seleccione una opción (a/b/c): ").lower()
    if sub_opcion == 'a':
        guia_id_str = input("Ingrese el ID de la guía de envío (UUID): ")
        try:
            gestion.ver_guias_envio_generadas(guia_id=guia_id_str)
        except ValueError:
            print("ID de guía inválido. Asegúrese de ingresar un UUID válido.")
    elif sub_opcion == 'b':
        sucursal_id_str = input("Ingrese el ID de la sucursal de origen (UUID): ")
        fecha_str = input("Ingrese la fecha (YYYY-MM-DD): ")
        try:
            gestion.ver_guias_envio_generadas(sucursal_origen_id=sucursal_id_str, fecha=fecha_str)
        except ValueError:
            print("ID de sucursal o fecha inválidos. Asegúrese de ingresar un UUID válido y una fecha en formato YYYY-MM-DD.")
    elif sub_opcion == 'c':
        gestion.ver_guias_envio_generadas()
    else:
        print("Opción no válida.")

def simular_falla_sucursal():
    print("\n=== SIMULAR FALLA DE SUCURSAL ===")
    print("1. Fallar ESTA sucursal (se apagará)")
    print("2. Fallar OTRA sucursal")
    opcion = input("Elige (1 o 2): ")

    if opcion == "1":
        print("\nEsta sucursal se marcará como INACTIVA...")
        if gestion.marcar_sucursal_inactiva(MY_ID):
            print("¡Adiós! Cerrando programa en 3 segundos.")
            time.sleep(3)
            sys.exit(0)
        else:
            print("Error: No se pudo marcar como inactiva")

    elif opcion == "2":
        print("\nSucursales activas disponibles:")
        sucursales = gestion.obtener_sucursales_activas()
        
        for suc_id in sucursales:
            if suc_id != MY_ID:
                print(f"- ID: {suc_id}")

        suc_fallida = input("\nIngresa el ID a fallar: ").strip()
        
        if suc_fallida in sucursales:
            print(f"\nSimulando falla en sucursal {suc_fallida}...")
            if gestion.marcar_sucursal_inactiva(suc_fallida):
                # Notificar a otros nodos
                for node_id, (ip, port) in ALL_NODES_INFO.items():
                    if node_id != MY_ID and node_id != suc_fallida:
                        try:
                            send_message(MY_ID, node_id, ip, port, f"NODO_CAIDO:{suc_fallida}")
                        except:
                            print(f"Error al notificar a {node_id}")
                print("¡Sucursal marcada como caída!")
            else:
                print("Error al actualizar Cassandra")
        else:
            print("¡ID inválido o sucursal ya caída!")
    else:
        print("Opción no válida")

def generar_guia_envio():
    sucursal_origen_id = input("Sucursal origen ID: ")
    sucursal_destino_id = input("Sucursal destino ID: ")
    articulo_id = input("Artculo ID: ")
    cantidad = int(input("cantidad: "))
    if gestion.verificar_stock_local >= cantidad:
        gestion.generar_guia(cliente_id, sucursal_origen_id, sucursal_destino_id, articulo_id, cantidad)
        gestion.actualizar_stock(sucursal_origen_id, articulo_id, -1*cantidad)
        gestion.actualizar_stock(sucursal_destino_id, articulo_id, cantidad)
        print("Guía generada")
    else:
        print("Stock insuficiente")
# --- Menú Interactivo del Sistema Distribuido ---
#Funcion de nodo maestro antes de main menu
def iniciar_eleccion_maestro():
    global IS_MASTER
    
    zk = KazooClient(hosts=ZOOKEEPER_HOSTS)  # Usar los hosts configurados
    zk.start()
    
    def lider_elegido():
        global IS_MASTER
        IS_MASTER = True
        print(f"\n[NODO MAESTRO] ID={MY_ID}")
        
        # Registrar como maestro en ZooKeeper
        try:
            zk.create(f"{ELECTION_PATH}/leader", 
                     value=MY_IP.encode(), 
                     ephemeral=True)
        except Exception as e:
            print(f"Error al registrarse como líder: {e}")
        
        # Tareas del maestro
        while IS_MASTER:
            time.sleep(10)
            if IS_MASTER:  # Verificar que aún sea maestro
                print("[MAESTRO] Monitoreando nodos...")
    
    def vigilar_maestro(data, stat):
        global IS_MASTER
        if not data:  # Si no hay maestro
            print("\n¡No hay maestro! Iniciando nueva elección...")
            IS_MASTER = False
            election.run(lider_elegido)
    
    # Configurar watch para detectar caídas del maestro
    zk.DataWatch(f"{ELECTION_PATH}/leader", vigilar_maestro)
    
    # Iniciar la elección
    election = Election(zk, ELECTION_PATH, identifier=MY_ID.encode())
    election.run(lider_elegido)
    
    #aqui
def registrar_nodo_efimero():
    zk = KazooClient(hosts=ZOOKEEPER_HOSTS)  # Conexión inicial local
    zk.start()
    
    # Registra tu IP en la lista de nodos
    zk.create(f"/nodos_efimeros_cassandra/nodo_{MY_ID}", 
              value=MY_IP.encode(), 
              ephemeral=True)
    
    # Si eres maestro, regístralo también
    if IS_MASTER:
        zk.create("/eleccion_maestro_cassandra/leader", 
                 value=MY_IP.encode(), 
                 ephemeral=True)
def monitorear_maestro():
    global IS_MASTER
    
    while True:
        if IS_MASTER:
            # Verificar si sigue siendo maestro
            zk = KazooClient(hosts=ZOOKEEPER_HOSTS)
            zk.start()
            try:
                data, stat = zk.get(f"{ELECTION_PATH}/leader")
                if data.decode() != MY_IP:
                    IS_MASTER = False
                    print("\n¡Ya no soy el maestro!")
            except:
                IS_MASTER = False
                print("\n¡Perdí el liderazgo!")
            finally:
                zk.stop()
        
        time.sleep(5)
        
def main_menu():
    while True:
        print("\n=== MENÚ PRINCIPAL DEL SISTEMA DISTRIBUIDO DE INVENTARIO ===")
        print("1. Consultar inventario local")
        print("2. Consultar inventario distribuido")
        print("3. Agregar artículo al inventario distribuido")
        print("4. Consultar lista de clientes")
        print("5. Agregar/Actualizar cliente")
        print("6. Comprar artículo (exclusión mutua)")
        print("7. Ver guías de envío generadas")
        print("8. Simular falla de sucursal")
        print("9. Generar guía de envío")
        print("10. Mostrar N últimos mensajes")
        print("11. ¿Es maestro?")
        print("0. Salir")

        opcion = input("Selecciona una opción: ").strip()

        if opcion == "1":
            consultar_inventario_local()
        elif opcion == "2":
            consultar_inventario_distribuido()
        elif opcion == "3":
            agregar_articulo_distribuido()
        elif opcion == "4":
            consultar_clientes()
        elif opcion == "5":
            actualizar_cliente()
        elif opcion == "6":
            comprar_articulo()
        elif opcion == "7":
            ver_guias_envio()
        elif opcion == "8":
            simular_falla_sucursal()
        elif opcion == "9":
            generar_guia_envio()
        elif opcion == "10":
            show_messages()
        elif opcion == "11":
            print("Sí" if IS_MASTER else "No")
        elif opcion == "0":
            print("Saliendo del sistema distribuido...")
            break
        else:
            print("Opción no válida. Intenta de nuevo.")
# ---- nuevo ----
def iniciar_servicios_maestro():
    global IS_MASTER
    while True:
        if IS_MASTER:
            print("🔷 [MAESTRO] Ejecutando tareas...")
            # Aquí puedes añadir más tareas del maestro
            time.sleep(10)

# --- Programa Principal ---
if __name__ == "__main__":
    MY_ID, MY_IP, MY_PORT, gestion, sucursal_id = get_node_info()
    #iniciar elección 
    eleccion_maestro_thread = threading.Thread(target=iniciar_eleccion_maestro, daemon=True)
    eleccion_maestro_thread.start()
    # Iniciar thread para recibir mensajes
    receive_thread = threading.Thread(target=receive_messages, args=(MY_ID, MY_PORT), daemon=True)
    receive_thread.start()

    # Si soy maestro, iniciar thread para tareas periódicas
    if IS_MASTER:
        maestro_thread = threading.Thread(target=maestro_periodico, daemon=True)
        maestro_thread.start()
    #servicios_thread = threading.Thread(target=iniciar_servicios_maestro, daemon=True)
    #servicios_thread.start()
    main_menu()

