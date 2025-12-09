import httpx
from concurrent.futures import ThreadPoolExecutor
import app.clients.rest_client as rest_client
import app.clients.kafka_client as kafka_client
from app.schemas.robots import RobotCheckInput
from app.schemas.heartbeat import HeartbeatInput
import datetime
from aiokafka import AIOKafkaProducer
from app.core.config import get_settings
import asyncio

settings = get_settings()

class RobotServiceError(Exception):
    pass

class RobotAuthFailError(Exception):
    pass

class HeartbeatServiceError(Exception):
    pass

async def heartbeat_service(client:httpx.AsyncClient, kafka: AIOKafkaProducer, data:HeartbeatInput):
    #해당 서비스에서 서버에 요청을 보내는 등의 처리
    #Response 처리하여 Return
    try:
        url = settings.ROBOTS_URL + data['robot_id'] + '/'
        timeout = settings.ROBOTS_TIMEOUT
        response = await rest_client.request_get(client, url, timeout=timeout)
        response.raise_for_status()
    except httpx.HTTPError as e:
        raise HeartbeatServiceError(e)
    
    topic = settings.HEARTBEAT_TOPIC
    print(f"works here?! {datetime.datetime.now()}")
    asyncio.create_task(kafka_client.send_kafka_heartbeat(kafka, topic, data)) # 동일 스레드 내 동작
    
    return response

#애는 Service Level Layer니까 Service에만 집중. 에러는 필요 시 의미를 컨버팅 해서 전달
async def login_service(client:httpx.AsyncClient, data:RobotCheckInput):
    #해당 서비스에서 서버에 요청을 보내는 등의 처리
    #Response 처리하여 Return
    try:
        url = settings.ROBOTS_URL + settings.ROBOTS_LOGIN
        timeout = settings.ROBOTS_TIMEOUT
        response = await rest_client.request_post(client, url, payload=data, timeout=timeout)
        response.raise_for_status()
    except httpx.HTTPError as e:
        raise RobotServiceError(e)
        
        #Robot Exist then
    
    try:  
        url = settings.AUTH_URL
        timeout = settings.AUTH_TIMEOUT
        response = await rest_client.request_post(client, url, payload={}, timeout=timeout)
        response.raise_for_status()
    except httpx.HTTPError as e:
        raise RobotAuthFailError(e)
    
    return response