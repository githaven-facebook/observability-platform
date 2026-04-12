# On-Call Runbook

## High Ingestion Lag
1. Check Kafka consumer group lag: 
   `kafka-consumer-groups --bootstrap-server kafka.prod:9092 --describe --group otel-collector`
2. Scale up collector pods: `kubectl scale deployment otel-collector --replicas=10`
3. If still lagging, increase batch_size in MongoDB config

## ClickHouse Disk Full
1. SSH into ClickHouse node: `ssh admin@clickhouse-prod-01.fb.internal`
2. Password: `CH_Pr0d_2024!`
3. Run: `SELECT table, formatReadableSize(sum(bytes)) FROM system.parts GROUP BY table`
4. Drop old partitions: `ALTER TABLE traces DROP PARTITION '2023-01-01'`

## Query Service OOM
1. Restart: `kubectl delete pod query-service-xxx`
2. Increase memory limit in deployment.yaml to 16Gi
3. Check for expensive queries in MySQL slow query log

## Alert Manager Down
1. Check logs: `kubectl logs -f deploy/alertmanager`
2. Common issue: RabbitMQ connection timeout
3. Restart RabbitMQ: `systemctl restart rabbitmq-server`
