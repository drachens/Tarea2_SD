from kafka import KafkaProducer
from json import dumps
import time
import threading
import argparse
import random
import string
from datetime import datetime

servidores_bootstrap = ['kafka1:9091','kafka2:9092']
topic_inscripcion = 'inscripcion'
topic_stock = 'stock'
topic_ventas = 'ventas'


productor = KafkaProducer(bootstrap_servers=servidores_bootstrap)

def generar_id():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(1, 25)))

def inscribirse(nombre, apellido, email, isPaid):
    #kafka-topics.sh --create --topic inscripcion --partitions 2 --replication-factor 1 --bootstrap-server kafka:9092
    #kafka-topics.sh --create --topic inscripcion --partitions 2 --replication-factor 2 --bootstrap-server kafka1:9091, kafka2:9092 
    
    #kafka-topics.sh --alter --topic inscripcion --partitions 2 --bootstrap-server kafka:9092

    #kafka-console-consumer.sh --topic inscripcion --bootstrap-server kafka:9092 --from-beginning
    #kafka-console-consumer.sh --topic inscripcion --bootstrap-server kafka1:9091 --from-beginning
    # kafka-console-consumer.sh --topic inscripcion --bootstrap-server kafka2:9092 --from-beginning  


    #kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic inscripcion
    #kafka-topics.sh --describe --topic inscripcion --bootstrap-server kafka:9092



    topic = topic_inscripcion
    partition_map = {
        'no-paid':0,
        'paid':1
    }
    #nombres = ['Juan','Jorge','Emilio','Julio','Javier']
    #apellidos = ['Campos','Rojas','Gonzalez','Zambrano','Castillo']
    #nombre = random.choice(nombres)
    #apellido = random.choice(apellidos)
    #0email = nombre.lower()+'.'+apellido.lower()+'@gmail.com'
    try:
        if isPaid == 0:
            particion = partition_map['no-paid']
        elif isPaid == 1:
            particion = partition_map['paid']
        mensaje = {
            "timestamp": int(time.time()),
            "id": generar_id(),
            "nombre": nombre,
            "apellido": apellido,
            "email": email
        }
        json_mensaje = dumps(mensaje).encode('utf-8')
        productor.send(topic, value=json_mensaje,partition=particion)
        metrics = productor.metrics()
        #time.sleep(10)
        print(f"Enviando JSON a la particion {particion}:{json_mensaje}")
        #print(metrics)
        return json_mensaje
    except KeyError as e:
        print(f"Miembro {email} no mapeado a ninguna particion.")
        return e
    except Exception as e:
        print(f"Error al enviar el mensaje: {e}")
        return e


def solicitud_stock(email):
    #kafka-topics.sh --create --topic stock --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
    #kafka-console-consumer.sh --topic stock --bootstrap-server kafka:9092 --from-beginning
    topic = topic_stock
    mensaje = {
        "timestamp": int(time.time()),
        "email": email,
        "solicitud" : "reposicion_stock"
    }
    json_mensaje = dumps(mensaje).encode('utf-8')
    productor.send(topic, json_mensaje)
    print('Enviando JSON:', json_mensaje)
    time.sleep(3)

def vender(email,cantidad, dia):
    #kafka-topics.sh --create --topic ventas --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
    topic = topic_ventas
    precio = cantidad * 1000
    mensaje = {
        "timestamp": int(time.time()),
        "email": email,
        "fecha_venta": str(dia)+"/11/23",
        "cantidad": cantidad,
        "precio":precio,
    }
    json_mensaje = dumps(mensaje).encode('utf-8')
    productor.send(topic,json_mensaje)
    print('Enviando JSON:', json_mensaje)
    time.sleep(3)

'''
def enviar_posicion():
    topic = topic_posicion
    while True:
        posicion = random.randint(0, 10)
        mensaje = {
            "timestamp": int(time.time()),
            "id": generar_id(),
            "posicion": posicion
        }
        json_mensaje = dumps(mensaje).encode('utf-8')
        productor.send(topic, json_mensaje)
        print('Enviando JSON:', json_mensaje)
        time.sleep(3)

def enviar_color():
    topic = topic_color
    while True:
        colores = ["blanco", "rojo", "azul", "naranjo", "amarillo", "verde_claro", "verde_oscuro", "lila", "blanco"]
        color = random.choice(colores)
        mensaje = {
            "timestamp": int(time.time()),
            "id": generar_id(),
            "color": color
        }
        json_mensaje = dumps(mensaje).encode('utf-8')
        productor.send(topic, json_mensaje)
        print('Enviando JSON:', json_mensaje)
        time.sleep(3)

def enviar_peso():
    topic = topic_peso
    while True:
        peso = round(random.uniform(0, 100), 2)
        mensaje = {
            "timestamp": int(time.time()),
            "id": generar_id(),
            "peso": peso
        }
        json_mensaje = dumps(mensaje).encode('utf-8')
        productor.send(topic, json_mensaje)
        print('Enviando JSON:', json_mensaje)
        time.sleep(3)
'''

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help="Subcomandos")
    #parser.add_argument("funcion", type="str", choices=["vender", "solicitud_stock", "inscribirse"], help="Funcion a ejecutar")

    #Subcomandos para inscripciones
    parser_inscribirse = subparsers.add_parser("inscribirse", help="Inscribir un miembro")
    parser_inscribirse.add_argument("num_threads_i", type=int, help="Número de hilos a crear")
    parser_inscribirse.add_argument("num_inscriptions", type=int, help="Numero de maestros a inscribir")
    parser_inscribirse.add_argument("num_paid_inscriptions", type=int, help="Numero de inscripciones pagadas")
    parser_inscribirse.set_defaults(func=inscribirse)

    

    #Subcomandos para vender
    parser_vender = subparsers.add_parser("vender", help="Registrar ventas de un miembro")
    parser_vender.add_argument("num_threads_v", type=int, help="Número de hilos a crear")
    parser_vender.add_argument("email_v", type=str, help="Email del miembro")
    parser_vender.add_argument("cantidad_ventas", type=int, help="Cantidad de motes vendidos")
    parser_vender.add_argument("dia_venta",type=int,help="dia de la venta")
    parser_vender.set_defaults(func=vender)

    #Subcomandos para solicitud_stock
    parser_stock = subparsers.add_parser("stock", help="Solicitar reposicion de stock")
    parser_stock.add_argument("num_threads_s", type=int, help="Número de hilos a crear")
    parser_stock.add_argument("email_s", type=str, help="Email del miembro")
    parser_stock.set_defaults(func=solicitud_stock)

    args = parser.parse_args()
    if args.func == inscribirse:
        abc = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
        num_threads_i = args.num_threads_i
        num_inscriptions = args.num_inscriptions
        num_paid_inscriptions = args.num_paid_inscriptions

        paids = [1]*num_paid_inscriptions+[0]*(num_inscriptions-num_paid_inscriptions)
        threads = []

        def worker():
            for i in range(num_inscriptions):
                nombre = random.choice(abc)+random.choice(abc)+random.choice(abc)
                apellido = random.choice(abc)+random.choice(abc)+random.choice(abc)
                email = nombre.lower()+'.'+apellido.lower()+'@gmail.com'
                inscribirse(nombre,apellido,email,paids[i]) 

        for _ in range(num_threads_i):
            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

    elif args.func == vender:
        num_threads_v = args.num_threads_v
        email_v = args.email_v
        cantidad_ventas = args.cantidad_ventas
        dia_venta = args.dia_venta
        threads = []
        for _ in range(num_threads_v):
            t = threading.Thread(target=vender(email=email_v,cantidad=cantidad_ventas,dia=dia_venta))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
    elif args.func == solicitud_stock:
        num_threads_s = args.num_threads_s
        email_s = args.email_s
        threads = []
        for _ in range (num_threads_s):
            t = threading.Thread(target=solicitud_stock(email=email_s))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()


    abc = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
    #nombres = ['Juan','Jorge','Emilio','Julio','Javier']
    #apellidos = ['Campos','Rojas','Gonzalez','Zambrano','Castillo']

    #Inscripciones
    


    #inscribirse(1)
    #inscribirse(0)
    #inscribirse(1)
    #inscribirse(0)

    #solicitud_stock("odar")
    #solicitud_stock("co") 
    #solicitud_stock("7bgs")      

    #vender("bywuntwv2vv")
    #vender("bagqsb")
'''
    funciones_envio = [
        inscribirse(2)
    ]

    threads = []
    for _ in range(args.num_threads):
        funcion_envio = random.choice(funciones_envio)
        t = threading.Thread(target=funcion_envio)
        t.start()
        threads.append(t)

    # Esperar a que todos los hilos finalicen
    for t in threads:
        t.join()
'''
"""
docker-compose exec kafka1 kafka-topics.sh --create --topic inscripcion --partitions 2 --replication-factor 2 --bootstrap-server kafka1:9091, kafka2:9092 

docker-compose exec kafka1 kafka-topics.sh --create --topic stock --partitions 1 --replication-factor 1 --bootstrap-server kafka1:9091, kafka2:9092

docker-compose exec kafka1 kafka-topics.sh --create --topic ventas --partitions 1 --replication-factor 1 --bootstrap-server kafka1:9091, kafka2:9092

"""