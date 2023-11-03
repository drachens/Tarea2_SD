from confluent_kafka import Consumer, KafkaError, TopicPartition
import json
import csv
import os
import threading

#INSCRIPCION
server = 'kafka1:9091,kafka2:9092'
archivo_csv = "miembros.csv"
encabezado_miembros = ['id','nombre','apellido','email','stock']
if not os.path.exists(archivo_csv):
    with open(archivo_csv, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=encabezado_miembros)
        writer.writeheader()

def consumir_inscripcion():
    topic_inscripcion = 'inscripcion'
    group_id_inscripcion = 'consumidores_inscripcion'

    conf = {
        'bootstrap.servers':server,
        'group.id':group_id_inscripcion,
        'auto.offset.reset': 'earliest',
    }

    consumer = Consumer(conf)

    asignacion = [TopicPartition(topic_inscripcion,1), TopicPartition(topic_inscripcion,0)]

    consumer.assign(asignacion)

    try:
        while True:
            msg = consumer.poll(timeout=1000)  # Espera mensajes durante 1 segundo
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Fin de la partición, no es un error
                    continue
                else:
                    print(f"Error al recibir mensaje: {msg.error()}")
                    break

            # Procesar el mensaje JSON
            try:
                data = json.loads(msg.value().decode('utf-8'))
                email = data.get('email')
                nombre = data.get('nombre')
                apellido = data.get('apellido')
                id = data.get('id')

                print(f"ID: {id}, Nombre: {nombre}, Apellido: {apellido}, Email: {email}")
                nuevo_registro = {'id':id, 'nombre':nombre, 'apellido':apellido, 'email':email, 'stock':10}
                #Verificar
                with open(archivo_csv,mode='r',newline='') as file:
                    reader = csv.DictReader(file)
                    filas = list(reader)
                #Verificar existencia correo
                email_existe = any(row.get('email') == email for row in filas if 'email' in row)

                if email_existe:
                    print(f'Correo electronico {email} ya existe. No se añadirá al registro.')
                else:
                    with open(archivo_csv, mode='a', newline='') as file:
                        writer = csv.DictWriter(file, fieldnames=encabezado_miembros)
                        writer.writerow(nuevo_registro)
                        print("Archivo escrito.")
            except json.JSONDecodeError as e:
                print(f"Error al decodificar JSON: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

#STOCK
def consumir_stock():
    topic_stock = 'stock'
    group_id_stock = 'consumidores_stock'
    conf_stock = {
        'bootstrap.servers':server,
        'group.id':group_id_stock,
        'auto.offset.reset': 'earliest',
    }
    consumer_stock = Consumer(conf_stock)
    consumer_stock.subscribe([topic_stock])
    try:
        while True:
            msg = consumer_stock.poll(timeout=1000)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error al recibir mensaje: {msg.error()}")
                    break

            # Procesar el mensaje JSON
            try:
                data = json.loads(msg.value().decode('utf-8'))
                solicitud = data.get('solicitud')
                email = data.get('email')

                print(f"EMAIL_MEMBER: {email}, Solicitud : {solicitud}")
                with open("miembros.csv",mode='r',newline='') as file:
                    reader = csv.DictReader(file)
                    filas = list(reader)

                fila_encontrada = False
                
                for fila in filas:
                    if fila.get('email') == email:
                        print(f'Evaluando la fila con ID {fila.get("email")}')
                        fila['stock'] = 10
                        fila_encontrada = True
                        break
                    
                if fila_encontrada:
                    with open(archivo_csv,mode='w',newline='') as file:
                        writer = csv.DictWriter(file,fieldnames=encabezado_miembros)
                        writer.writeheader()
                        writer.writerows(filas)
                        print(f'Se ha modificado el valor de stock para el miembro {email} a 10')
                else:
                    print(f'No se encontro la fila con email {email}')
            except json.JSONDecodeError as e:
                print(f"Error al decodificar JSON: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer_stock.unsubscribe()
        consumer_stock.close()

def consumir_ventas():

    archivo_csv = "ventas.csv"
    encabezado = ['email_member','fecha_venta','cantidad','precio']
    if not os.path.exists(archivo_csv):
        with open(archivo_csv, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=encabezado)
            writer.writeheader()

    topic_ventas = 'ventas'
    group_id_ventas = 'consumidores_ventas'
    conf_ventas = {
        'bootstrap.servers':server,
        'group.id':group_id_ventas,
        'auto.offset.reset': 'earliest',     
    }
    consumer_ventas = Consumer(conf_ventas)
    consumer_ventas.subscribe([topic_ventas])
    try:
        while True:
            msg = consumer_ventas.poll(timeout=1000)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error al recibir mensaje: {msg.error()}")
                    break

            # Procesar el mensaje JSON
            try:
                data = json.loads(msg.value().decode('utf-8'))
                email = data.get('email')
                fecha_venta = data.get('fecha_venta')
                cantidad = data.get('cantidad')
                precio = data.get('precio')

                print(f"Venta de miembro {email} con fecha {fecha_venta} de {cantidad} huesillo/s en ${precio}")
                nuevo_registro = {'email_member':email, 'fecha_venta':fecha_venta, 'cantidad':cantidad, 'precio':precio}
                with open(archivo_csv, mode='a', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=encabezado)
                    writer.writerow(nuevo_registro)
                    print("Archivo de ventas escrito.")

                with open("miembros.csv",mode='r',newline='') as file:
                    reader = csv.DictReader(file)
                    filas = list(reader)

                fila_encontrada = False
                
                for fila in filas:
                    if fila.get('email') == email:
                        print(f'Evaluando la fila con EMAIL {fila.get("email")}')
                        nuevo_stock = 0 if cantidad>=int(fila['stock']) else int(fila['stock']) - cantidad
                        fila['stock'] = str(nuevo_stock)
                        fila_encontrada = True
                        break
                    
                if fila_encontrada:
                    with open("miembros.csv",mode='w',newline='') as file:
                        writer = csv.DictWriter(file,fieldnames=encabezado_miembros)
                        writer.writeheader()
                        writer.writerows(filas)
                        print(f'Se ha modificado el valor de stock para la EMAIL {email} a {nuevo_stock}')
                
            except json.JSONDecodeError as e:
                print(f"Error al decodificar JSON: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer_ventas.unsubscribe()
        consumer_ventas.close()   
        
hilo_inscripcion = threading.Thread(target=consumir_inscripcion)
hilo_stock = threading.Thread(target=consumir_stock)
hilo_ventas = threading.Thread(target=consumir_ventas)


hilo_inscripcion.start()
hilo_stock.start()
hilo_ventas.start()

hilo_inscripcion.join()
hilo_stock.join()
hilo_ventas.join()