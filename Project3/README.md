klassi töö_1
nr1
docker compose up -d

docker compose ps

nr2

docker exec jupyter python /home/jovyan/project/seed.py

nr3 Start the simulator (keep it running in a separate terminal):

docker exec jupyter python /home/jovyan/project/simulate.py

from a host terminal:

docker exec jupyter python /home/jovyan/project/produce.py --loop

nr4

curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json"  --data-binary "@cdc.json"

nr5

Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:8083/connectors/cdc-connector/status" | Select-Object -ExpandProperty Content

nr6

docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dbserver1.public.customers --from-beginning --max-messages 3

nr 7  
if needed change airflow password
docker compose exec airflow airflow users reset-password --username admin --password bdmgroupc
