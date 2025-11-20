"""
Kafka Consumer with Avro Deserialization
Consumes order messages from the 'orders' topic with:
- Real-time price aggregation (running average)
- Retry logic for temporary failures
- Dead Letter Queue (DLQ) for permanently failed messages
"""

import json
import time
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


def load_avro_schema(schema_path):
    """Load Avro schema from file"""
    with open(schema_path, 'r') as f:
        return f.read()


class OrderConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', schema_registry_url='http://localhost:8081', 
                 group_id='order-consumer-group'):
        """Initialize Kafka consumer with Avro deserialization"""
        
        # Consumer configuration
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commit for retry logic
        }
        
        self.consumer = Consumer(consumer_config)
        
        # Producer for DLQ
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
        }
        self.dlq_producer = Producer(producer_config)
        
        # Schema Registry client
        schema_registry_conf = {'url': schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Load Avro schema
        avro_schema_str = load_avro_schema('../schemas/order.avsc')
        
        # Create Avro deserializer
        self.avro_deserializer = AvroDeserializer(
            schema_registry_client,
            avro_schema_str,
            lambda data, ctx: data
        )
        
        self.topic = 'orders'
        self.dlq_topic = 'orders-dlq'
        
        # Statistics for running average
        self.total_price = 0.0
        self.message_count = 0
        self.running_average = 0.0
        
        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 2  # seconds
    
    def calculate_running_average(self, new_price):
        """Calculate running average of prices"""
        self.message_count += 1
        self.total_price += new_price
        self.running_average = self.total_price / self.message_count
        return self.running_average
    
    def send_to_dlq(self, message, error_reason):
        """Send failed message to Dead Letter Queue"""
        try:
            dlq_payload = {
                'original_topic': message.topic(),
                'original_partition': message.partition(),
                'original_offset': message.offset(),
                'error_reason': error_reason,
                'timestamp': int(time.time()),
                'key': message.key().decode('utf-8') if message.key() else None,
                'value': message.value()
            }
            
            self.dlq_producer.produce(
                topic=self.dlq_topic,
                key=message.key(),
                value=json.dumps(dlq_payload).encode('utf-8')
            )
            self.dlq_producer.flush()
            print(f'💀 Message sent to DLQ: {error_reason}')
            
        except Exception as e:
            print(f'❌ Error sending to DLQ: {e}')
    
    def process_message(self, order, attempt=1):
        """
        Process a single order message with retry logic
        Returns True if successful, False if failed
        """
        try:
            # Simulate occasional processing failures for demonstration
            # In real scenario, this could be database errors, API failures, etc.
            if order['orderId'] in ['5', '8']:  # Simulate temporary failures
                if attempt <= 2:
                    raise Exception(f"Temporary processing error (attempt {attempt})")
            
            if order['orderId'] in ['3']:  # Simulate permanent failure
                raise Exception("Permanent processing error - invalid order format")
            
            # Process the order
            avg_price = self.calculate_running_average(order['price'])
            
            print(f'\n📨 Order Processed:')
            print(f'   Order ID: {order["orderId"]}')
            print(f'   Product: {order["product"]}')
            print(f'   Price: ${order["price"]:.2f}')
            print(f'   📊 Running Average Price: ${avg_price:.2f}')
            print(f'   📈 Total Orders Processed: {self.message_count}')
            
            return True
            
        except Exception as e:
            print(f'⚠️  Error processing order {order.get("orderId", "unknown")} (attempt {attempt}): {e}')
            return False
    
    def consume_messages(self):
        """Consume messages with retry logic and DLQ"""
        
        # Subscribe to topic
        self.consumer.subscribe([self.topic])
        
        print(f'\n🎧 Consumer started. Listening to topic: {self.topic}')
        print(f'📊 Real-time aggregation enabled (running average of prices)')
        print(f'🔄 Retry logic enabled (max {self.max_retries} retries)')
        print(f'💀 Dead Letter Queue: {self.dlq_topic}\n')
        print('Press Ctrl+C to stop...\n')
        
        try:
            while True:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition
                        continue
                    else:
                        raise KafkaException(msg.error())
                
                # Deserialize message
                try:
                    order = self.avro_deserializer(
                        msg.value(),
                        SerializationContext(self.topic, MessageField.VALUE)
                    )
                    
                    # Process with retry logic
                    success = False
                    for attempt in range(1, self.max_retries + 1):
                        success = self.process_message(order, attempt)
                        
                        if success:
                            # Commit offset after successful processing
                            self.consumer.commit(asynchronous=False)
                            break
                        else:
                            if attempt < self.max_retries:
                                print(f'🔄 Retrying in {self.retry_delay} seconds... (attempt {attempt + 1}/{self.max_retries})')
                                time.sleep(self.retry_delay)
                    
                    # If all retries failed, send to DLQ
                    if not success:
                        print(f'❌ All retry attempts failed for order {order.get("orderId", "unknown")}')
                        self.send_to_dlq(msg, f'Failed after {self.max_retries} retry attempts')
                        # Commit offset to move past this message
                        self.consumer.commit(asynchronous=False)
                
                except Exception as e:
                    print(f'❌ Deserialization error: {e}')
                    self.send_to_dlq(msg, f'Deserialization error: {str(e)}')
                    # Commit offset to move past this corrupted message
                    self.consumer.commit(asynchronous=False)
        
        except KeyboardInterrupt:
            print('\n\n🛑 Consumer stopped by user')
        
        finally:
            # Close consumer
            self.consumer.close()
            print('✅ Consumer closed')
            print(f'\n📊 Final Statistics:')
            print(f'   Total Orders Processed: {self.message_count}')
            print(f'   Average Price: ${self.running_average:.2f}\n')


if __name__ == '__main__':
    # Create consumer
    consumer = OrderConsumer()
    
    # Start consuming messages
    consumer.consume_messages()
