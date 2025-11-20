# 1. Start Kafka services
docker-compose up -d

# 2. Wait 30 seconds
Start-Sleep -Seconds 30

# 3. Create topics
docker exec -it kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 4. Install Python packages
pip install -r requirements.txt

# 5. Run consumer (Terminal 1)
cd src
python consumer.py

# 6. Run producer (Terminal 2)
cd src
python producer.py

# What You'll See:
Producer Output:

    -10 orders generated with random products and prices
    -Each message confirmed delivered to Kafka

Consumer Output:

    -Real-time processing of orders
    -Running average calculation displayed
    -Order #3: Permanent failure → sent to DLQ (with 3 retries)
    -Orders #5 & #8: Temporary failures → retry and succeed
    -Final statistics shown