import socket
import threading
from datetime import datetime
import os
import sys
import netifaces

# --- Configuración Global ---
CONFIG_FILE = "config.txt"
MESSAGES = []
MY_ID = None
MY_IP = None
MY_PORT = None
ALL_NODES_INFO = {}

# --- Funciones Utilitarias ---
def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

def get_node_info():
    global MY_ID, MY_IP, MY_PORT, ALL_NODES_INFO
    try:
        with open(CONFIG_FILE, "r") as f:
            for line in f:
                parts = line.strip().split(",")
                if len(parts) == 3:
                    node_id, ip, port_str = parts
                    ALL_NODES_INFO[node_id] = (ip, int(port_str))

        interfaces = netifaces.interfaces()
        for interface in interfaces:
            try:
                addresses = netifaces.ifaddresses(interface)
                if socket.AF_INET in addresses:
                    for ip_info in addresses[socket.AF_INET]:
                        current_ip = ip_info['addr']
                        for node_id, (node_ip, node_port) in ALL_NODES_INFO.items():
                            if node_ip == current_ip:
                                MY_ID = node_id
                                MY_IP = node_ip
                                MY_PORT = node_port
                                print(f"Configuración local encontrada: {MY_ID} {MY_IP}:{MY_PORT}")
                                return MY_ID, MY_IP, MY_PORT
            except Exception as e:
                print(f"Error en interfaz {interface}: {e}")

    except FileNotFoundError:
        print(f"Error: Archivo '{CONFIG_FILE}' no encontrado")
        sys.exit(1)

    raise Exception("No se encontró configuración para esta IP local")

def store_message(message):
    if MY_ID is None:
        return

    filename = f"mensajes_{MY_ID}.txt"
    try:
        if not os.path.exists(filename):
            open(filename, 'w').close()

        with open(filename, "a", encoding='utf-8') as f:
            f.write(message + "\n")
    except Exception as e:
        print(f"Error al guardar mensaje: {e}")

# --- Funciones de Red ---
def receive_messages(my_id, my_port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("0.0.0.0", my_port))
    server_socket.listen(5)
    print(f"Nodo {my_id}: Escuchando en el puerto {my_port}...")

    while True:
        client_socket, addr = server_socket.accept()
        threading.Thread(target=handle_connection, args=(client_socket, my_id)).start()

def handle_connection(client_socket, my_id):
    try:
        message_data = client_socket.recv(1024).decode('utf-8')
        if message_data:
            sender_id, message_with_timestamp = message_data.split(":", 1)
            timestamp, message = message_with_timestamp.split("|", 1)
            full_message = f"[{get_timestamp()}] Nodo {sender_id}: {message}"
            print(full_message)
            store_message(f"[{timestamp}] Nodo {sender_id}: {message}")
            response = f"RECIBIDO por Nodo {my_id} a las {get_timestamp()}"
            client_socket.sendall(response.encode('utf-8'))
    except Exception as e:
        print(f"Nodo {my_id}: Error al recibir mensaje: {e}")
    finally:
        client_socket.close()

def send_message(my_id, target_name, target_ip, target_port, message_text):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.settimeout(5)
            client_socket.connect((target_ip, target_port))
            timestamp = get_timestamp()
            full_message = f"{my_id}:{timestamp}|{message_text}"
            client_socket.sendall(full_message.encode('utf-8'))
            store_message(f"[{timestamp}] Nodo {my_id}: {message_text} (Enviado)")
            response = client_socket.recv(1024).decode('utf-8')
            print(f"Respuesta de {target_name}: {response}")
    except socket.timeout:
        print(f"Error: Tiempo de espera agotado al conectar a {target_ip}:{target_port}")
    except ConnectionRefusedError:
        print(f"Error: Conexión rechazada por {target_ip}:{target_port}")
    except Exception as e:
        print(f"Nodo {my_id}: Error al enviar mensaje: {e}")

# --- Funciones para cada inciso (aún sin implementar) ---
def distribuir_articulos():
    print("[Función distribuir_articulos] Aquí implementa la lógica para distribuir artículos automáticamente entre sucursales.")

def consultar_actualizar_clientes():
    print("[Función consultar_actualizar_clientes] Aquí implementa la lógica para consultar y actualizar la lista de clientes distribuida.")

def comprar_articulo():
    print("[Función comprar_articulo] Aquí implementa la compra con exclusión mutua, generación y guardado de guía de envío, y actualización de inventario.")

def agregar_articulos():
    print("[Función agregar_articulos] Aquí implementa la lógica para agregar artículos al inventario distribuido y distribuir equitativamente.")

def actualizar_con_consenso():
    print("[Función actualizar_con_consenso] Aquí implementa la actualización con consenso para datos de inventario, clientes, etc.")

def redistribuir_falla():
    print("[Función redistribuir_falla] Aquí implementa la redistribución de artículos si una sucursal falla y actualizar la información.")

def eleccion_nodo_maestro():
    print("[Función eleccion_nodo_maestro] Aquí implementa la elección automática si el nodo maestro falla.")

# --- Menú ---
def menu(my_id):
    while True:
        print("\n--- MENÚ PRINCIPAL ---")
        print("1. Distribuir artículos entre sucursales (Nodo maestro)")
        print("2. Consultar y actualizar lista de clientes distribuida")
        print("3. Comprar artículo (con exclusión mutua y guía de envío)")
        print("4. Agregar artículos al inventario distribuido")
        print("5. Actualizar datos con consenso")
        print("6. Redistribuir artículos en caso de falla de sucursal")
        print("7. Elección de nodo maestro en caso de falla")
        print("0. Salir")

        opcion = input("Selecciona una opción: ").strip()

        if opcion == '1':
            distribuir_articulos()
        elif opcion == '2':
            consultar_actualizar_clientes()
        elif opcion == '3':
            comprar_articulo()
        elif opcion == '4':
            agregar_articulos()
        elif opcion == '5':
            actualizar_con_consenso()
        elif opcion == '6':
            redistribuir_falla()
        elif opcion == '7':
            eleccion_nodo_maestro()
        elif opcion == '0':
            print("Saliendo...")
            os._exit(0)
        else:
            print("Opción inválida, intenta de nuevo.")

# --- Entrada principal ---
if __name__ == "__main__":
    try:
        my_id, my_ip, my_port = get_node_info()
        print(f"Nodo {my_id} iniciado en {my_ip}:{my_port}")

        receive_thread = threading.Thread(
            target=receive_messages,
            args=(my_id, my_port),
            daemon=True
        )
        receive_thread.start()

        menu(my_id)

    except Exception as e:
        print(f"Error al iniciar el nodo: {e}")

