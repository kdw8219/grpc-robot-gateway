from app.core.config import get_settings
from aiokafka import AIOKafkaProducer

settings = get_settings()

async def send_kafka_heartbeat(kafka:AIOKafkaProducer, topic:str, payload:dict):
    try:
        result = await kafka.send_and_wait(topic, payload)
        print(f"Complete...: Topic={result.topic}, Partition={result.partition}, Offset={result.offset}")
    except Exception as e:
        #TODO 에러 재처리 로직, 또는 에러로 판단하여 후속 처리 로직 필요
        result = await kafka.send_and_wait(topic, payload)