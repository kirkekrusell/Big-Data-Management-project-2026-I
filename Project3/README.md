klassi töö_1
nr1
docker compose up -d

docker compose ps

nr2
docker exec jupyter python /home/jovyan/project/seed.py

nr3 Start the simulator (keep it running in a separate terminal):

docker exec jupyter python /home/jovyan/project/simulate.py

nr4
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "{
    \"name\": \"cdc-connector\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
      \"database.hostname\": \"postgres\",
      \"database.port\": \"5432\",
      \"database.user\": \"cdc_user\",
      \"database.password\": \"bdmgroupc\",
      \"database.dbname\": \"sourcedb\",
      \"topic.prefix\": \"dbserver1\",
      \"table.include.list\": \"public.customers,public.drivers\",
      \"plugin.name\": \"pgoutput\",
      \"slot.name\": \"debezium_slot\",
      \"publication.name\": \"dbz_publication\"
    }
  }"

nr5
Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:8083/connectors/cdc-connector/status" | Select-Object -ExpandProperty Content

nr6
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dbserver1.public.customers --from-beginning --max-messages 3
