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
        pass
