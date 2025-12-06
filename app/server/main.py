import grpc
from grpc.aio import server

import app.services.conn_service as conn_service
import app.generated.robot_api_gateway_pb2 as pb
import app.generated.robot_api_gateway_pb2_grpc as pb_grpc
import httpx
from concurrent.futures import ThreadPoolExecutor
import datetime
from aiokafka import AIOKafkaProducer
import json
from app.core.config import get_settings
import asyncio

class RobotGatewayService(pb_grpc.RobotApiGatewayServicer):
    
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.settings = get_settings()
        self.client = httpx.AsyncClient()
        self.kafka = AIOKafkaProducer(bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVER
                             , value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        
    def __del__(self):
        self.executor.shutdown(wait=False)
        self.client.aclose()
        self.kafka.stop()
    
    async def Login(self, request, context):
        
        if request.robot_id == "" or request.robot_secret == "":
            return pb.LoginResponse(
                success=False,
                access_token="",
                refresh_token="",
            )
        
        try:
            json_data = {
                'robot_id':request.robot_id,
                'robot_secret':request.robot_secret
            }
            login_response = await conn_service.login_service(self.client, data = json_data)
            login_response.raise_for_status()
            
            return pb.LoginResponse(
                success=True,
                access_token="" + login_response.json().get('access_token', ''),
                refresh_token="" + login_response.json().get('refresh_token', ''),
            )
            
        except Exception as e:
            return pb.LoginResponse(
                success=False,
                access_token="",
                refresh_token="",
            ) 
            
    async def Heartbeat(self, request, context):
        
        try:
            json_data = {
                'robot_id':request.robot_id,
                'is_alive':True,
                'stream_ip':request.internal_ip,
                'timestamp':datetime.datetime.now().isoformat()
            }
            login_response = await conn_service.heartbeat_service(self.client, self.executor, self.kafka, data = json_data)
            login_response.raise_for_status()
            
            return pb.HeartbeatResponse(
                success=True,
                access_token="" + login_response.json().get('access_token', ''),
                refresh_token="" + login_response.json().get('refresh_token', ''),
            )
            
        except Exception as e:
            return pb.HeartbeatResponse(
                success=False,
                access_token="",
                refresh_token="",
            )       
        
            
async def serve():
    print("Starting gRPC Robot API Gateway Server...")
    svr = server()
    pb_grpc.add_RobotApiGatewayServicer_to_server(RobotGatewwayService(), svr)
    print("gRPC Robot API Gateway Server started on port 50051")
    svr.add_insecure_port("[::]:50051")
    await svr.start()
    print("gRPC Robot API Gateway Server is running...")
    await svr.wait_for_termination()
    print("gRPC Robot API Gateway Server stopped.")
    
if __name__ == "__main__":
    
    asyncio.run(serve())    