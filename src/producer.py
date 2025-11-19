"""
Kafka Producer with Avro Serialization
Produces order messages to the 'orders' topic
"""

import json
import random
import time
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


def load_avro_schema(schema_path):
    """Load Avro schema from file"""
    with open(schema_path, 'r') as f:
        return f.read()


def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


class OrderProducer:
    def __init__(self, bootstrap_servers='localhost:9092', schema_registry_url='http://localhost:8081'):
        """Initialize Kafka producer with Avro serialization"""
        
        # Producer configuration
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
        }
        
        self.producer = Producer(producer_config)
        
        # Schema Registry client
        schema_registry_conf = {'url': schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Load Avro schema
        avro_schema_str = load_avro_schema('../schemas/order.avsc')
        
        # Create Avro serializer
        self.avro_serializer = AvroSerializer(
            schema_registry_client,
            avro_schema_str,
            lambda order, ctx: order
        )
        
        self.topic = 'orders'
        
        # Sample products
        self.products = [
            'Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones',
            'Webcam', 'USB Cable', 'SSD Drive', 'RAM Module', 'Graphics Card'
        ]
    
    def generate_order(self, order_id):
        """Generate a random order"""
        return {
            'orderId': str(order_id),
            'product': random.choice(self.products),
            'price': round(random.uniform(10.0, 500.0), 2)
        }
    
    def produce_order(self, order):
        """Produce a single order message"""
        try:
            # Serialize the order using Avro
            serialized_value = self.avro_serializer(
                order,
                SerializationContext(self.topic, MessageField.VALUE)
            )
            
            # Produce message
            self.producer.produce(
                topic=self.topic,
                key=order['orderId'].encode('utf-8'),
                value=serialized_value,
                on_delivery=delivery_report
            )
            
            # Trigger callbacks
            self.producer.poll(0)
            
        except Exception as e:
            print(f'❌ Error producing message: {e}')
    
    def produce_orders(self, count=10, delay=2):
        """Produce multiple orders with delay"""
        print(f'\n🚀 Starting to produce {count} orders...\n')
        
        for i in range(1, count + 1):
            order = self.generate_order(i)
            print(f'📦 Producing Order #{i}: {order}')
            self.produce_order(order)
            
            if i < count:
                time.sleep(delay)
        
        # Wait for any outstanding messages to be delivered
        print('\n⏳ Waiting for messages to be delivered...')
        self.producer.flush()
        print('✅ All messages delivered!\n')
    
    def close(self):
        """Close the producer"""
        self.producer.flush()


if __name__ == '__main__':
    # Create producer
    producer = OrderProducer()
    
    # Produce 10 orders with 2 second delay between each
    producer.produce_orders(count=10, delay=2)
    
    # Close producer
    producer.close()
