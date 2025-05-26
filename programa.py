import os
import socket
import threading
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

# --- Funciones Utilitarias ---
def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

def get_node_info():
    global MY_ID, MY_IP, MY_PORT, ALL_NODES_INFO, sucursal_id, gestion
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
        raise Exception("No se encontró configuración para esta IP local")

        gestion = GestionInventario(contact_points=[MY_IP], keyspace='inventario_logistica')
        sucursal_id = gestion.obtener_sucursal_id(MY_IP)
    except FileNotFoundError:
        print(f"Error: Archivo '{CONFIG_FILE}' no encontrado")
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
    except Exception as e:
        print(f"Error al guardar mensaje: {e}")

# --- Funciones de Red ---
def receive_messages(my_id, my_port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', my_port))
    server_socket.listen()
    print(f"Nodo {my_id} está escuchando en el puerto {my_port}...")
    while True:
        client_socket, addr = server_socket.accept()
        threading.Thread(target=handle_connection, args=(client_socket, my_id)).start()

def handle_connection(client_socket, my_id):
    try:
        message_data = client_socket.recv(1024).decode('utf-8')
        if message_data:
            sender_id, message_with_timestamp = message_data.split(":", 1)
            timestamp, message = message_with_timestamp.split("||", 1)
            full_message = f"[{get_timestamp()}] Nodo {sender_id}: {message}"
            print(full_message)
            store_message(f"[{timestamp}] Nodo {sender_id}: {message} (Recibido)")
            response = f"Recibido por Nodo {my_id} a las {get_timestamp()}"
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
            full_message = f"{my_id}:{timestamp}||{message_text}"
            client_socket.sendall(full_message.encode('utf-8'))
            print(f"[{timestamp}] Nodo {my_id}: {message_text} (Enviado)")
            store_message(f"[{timestamp}] Nodo {my_id}: {message_text} (Enviado)")
            response = client_socket.recv(1024).decode('utf-8')
            print(f"Respuesta de {target_name}: {response}")
    except Exception as e:
        print(f"Nodo {my_id}: Error al enviar mensaje: {e}")

# --- Funciones del Sistema Distribuido ---
def consultar_inventario_local():
    global sucursal_id
    print("{"+sucursal_id+"}")
    gestion.consultar_inventario_local(sucursal_id)

def consultar_inventario_distribuido():
    gestion.consultar_inventario_distribuido()

def agregar_articulo_distribuido():
    print("[Funcionalidad en desarrollo] Agregar artículo al inventario distribuido")

def consultar_clientes():
    print("[Funcionalidad en desarrollo] Consultar lista de clientes")

def actualizar_cliente():
    print("[Funcionalidad en desarrollo] Agregar/Actualizar cliente")

def comprar_articulo():
    print("[Funcionalidad en desarrollo] Comprar artículo con exclusión mutua")

def ver_guias_envio():
    print("[Funcionalidad en desarrollo] Ver guías de envío generadas")

def simular_falla_sucursal():
    print("[Funcionalidad en desarrollo] Simular falla de sucursal")

def forzar_eleccion_maestro():
    print("[Funcionalidad en desarrollo] Forzar elección de nuevo nodo maestro")

# --- Menú Interactivo del Sistema Distribuido ---
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
        print("9. Forzar elección de nuevo nodo maestro")
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
            forzar_eleccion_maestro()
        elif opcion == "0":
            print("Saliendo del sistema...")
            break
        else:
            print("Opción no válida. Intenta de nuevo.")

# --- Programa Principal ---
if __name__ == "__main__":
    try:
        my_id, my_ip, my_port = get_node_info()
        threading.Thread(target=receive_messages, args=(my_id, my_port), daemon=True).start()
        gestion = GestionInventario(contact_points=['192.168.1.101'], keyspace='inventario_logistica')
        main_menu()
    except KeyboardInterrupt:
        print("\nPrograma interrumpido por el usuario.")
    except Exception as e:
        print(f"Error en ejecución principal: {e}")
