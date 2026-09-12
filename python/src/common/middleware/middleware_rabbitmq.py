import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name

    def send(self, message):
        # Se inicializa la conexion con rabbitmq de parte del productor
        producer_connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        # Se crea un canal dentro de la conexion establecida para no tener que establecer multiples conexiones 
        producer_channel = producer_connection.channel()

        # Creo una cola con el nombre que me pasan y el parametro durable true para que persista
        producer_channel.queue_declare(queue=self.queue_name, durable=True)
        # Se envia/publica el mensaje en rabbitmq, donde el mensaje se va a encolar segun lo definido en routing_key
        producer_channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
    
    def start_consuming(self, on_message_callback):
        # Se inicializa la conexion con rabbitmq de parte del consumidor y se crea el canal dentro de la conexion
        consumer_connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        consumer_channel = consumer_connection.channel()

        # Creo la cola donde se encolan los mensajes
        consumer_channel.queue_declare(queue=self.queue_name, durable=True)

        # Defino la funcion de callback para que cuando se reciba un mensaje de la cola se ejecute esta funcion
        # Es una funcion inermediaria para recibir los 4 parametros y transformar esa informacion
        def callback(channel, method, properties, body):
            # Defino las funciones closure ack y nack
            def ack():
                # Llamo al metodo basic_ack que manda un ack a rabbitmq confirmando que salio bien y que puede desencolar el mensaje
                # Le paso por parametro el codigo del mensaje (delvery_tag)
                channel.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                # Llamo al metodo basic_nack para avisar que hubo un error, defino requeue false para queno se encole nuevamente el mensaje
                # Le paso por parametro el codigo del mensaje (delvery_tag)
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            # Llamo a on_message_callback y paso por parametro las variables
            on_message_callback(body, ack, nack)

        # Defino que quiero consumir de la cola declarada con el nombre self.queue_name y paso la funcion callback
        consumer_channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
        # Empieza a recibir los mensajes de la cola
        consumer_channel.start_consuming()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

    def start_consuming(self, on_message_callback):
        # Inicializo conexion con rabbitmq y creo canal
        consumer_connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        consumer_channel = consumer_connection.channel()

        # Defino el exchanger con el nombre y tipo 'direct' que busca coincidencia exacta
        consumer_channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

        # Creo la cola donde se encolan los mensajes, en el queue name no le paso nada y lo genera automaticamente rabbitmq
        result = consumer_channel.queue_declare(queue='', exclusive=True)
        # Guardo el nombre generado
        queue_name = result.method.queue

        # Recorro todos los routing_keys que son todos los tipos/claves de mensajes que quiero que se encolen
        for rk in self.routing_keys:
            consumer_channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=rk)
        
        # Defino la funcion callback clousure con las funciones ack y nack dentro
        def callback(channel, method, properties, body):
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            on_message_callback(body, ack, nack)

        # Defino de que cola quiero consumir, la funcion callback que llamo cada vez que entra un mensaje y empeizo a consumir
        consumer_channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
        consumer_channel.start_consuming()

    def send(self, message):
        producer_connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        producer_channel = producer_connection.channel()

        producer_channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

        # Recorro todos los routing_keys y envio el mismo mensaje con cada clave diferente
        for rk in self.routing_keys:
            producer_channel.basic_publish(exchange=self.exchange_name, routing_key=rk, body=message)