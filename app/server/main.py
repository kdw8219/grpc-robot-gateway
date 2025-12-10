import grpc
from grpc.aio import server

import app.services.conn_service as conn_service
import app.generated.robot_api_gateway_pb2 as pb
import app.generated.robot_api_gateway_pb2_grpc as pb_grpc
import httpx
import datetime
from aiokafka import AIOKafkaProducer
import json
from app.core.config import get_settings
import asyncio

class RobotGatewayService(pb_grpc.RobotApiGatewayServicer):
    
    async def __aenter__(self, kafka_producer=None):
        self.settings = get_settings()
        self.client = httpx.AsyncClient()
        self.kafka = kafka_producer or AIOKafkaProducer(
            bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVER,
            value_serializer=lambda m: json.dumps(m).encode('utf-8'),
        )
        
        if isinstance(self.kafka, AIOKafkaProducer):   # 실제 producer일 때만 start
            await self.kafka.start()
            
        return self
        
    
    async def __aexit__(self, exc_type, exc, tb):
        self.executor.shutdown(wait=False)
        await self.client.aclose()
        await self.kafka.stop()
    
    async def Login(self, request, context):
        if request.robot_id == "" or request.robot_secret == "":
            return pb.LoginResponse(
                success=False,
                access_token="",
                refresh_token="",
            )
        
        try:
            json_data = {
                'robot_id': request.robot_id,
                'robot_secret': request.robot_secret,
            }
            login_response = await conn_service.login_service(self.client, data=json_data)
            login_response.raise_for_status()
            
            return pb.LoginResponse(
                success=True,
                access_token=login_response.json().get('access_token', ''),
                refresh_token=login_response.json().get('refresh_token', ''),
            )
        except Exception:
            return pb.LoginResponse(
                success=False,
                access_token="",
                refresh_token="",
            )
            
    async def Heartbeat(self, request, context):
        try:
            json_data = {
                'robot_id': request.robot_id,
                'is_alive': True,
                'timestamp': datetime.datetime.now().isoformat(),
            }
            heartbeat_response = await conn_service.heartbeat_service(
                self.client, self.kafka, json_data
            )
            heartbeat_response.raise_for_status()
            
            return pb.HeartbeatResponse(
                success=True,
                result=heartbeat_response.json().get('result', ''),
            )
        except asyncio.CancelledError:
            raise  # 이건 gRPC에 그대로 CancelledError로 전달됨
    
        except Exception as e:
            return pb.HeartbeatResponse(
                success=False,
                result=heartbeat_response.json().get('result', ''),
            )
            
    async def Pos(self, request, context):
        try:
            json_data = {
                'robot_id': request.robot_id,
                'x': float(request.x),
                'y': float(request.y),
                'z': float(request.z),
                'orig_x': float(request.orig_x),
                'orig_y': float(request.orig_y),
                'orig_z': float(request.orig_z),
                'orig_w': float(request.orig_w),
                'linear_angle': float(request.linear_angle),
                'angular_speed': float(request.angular_speed),
                'stream_ip': float(request.internal_ip),
                'timestamp': datetime.datetime.now().isoformat(),
            }
            await conn_service.pos_service(
                self.kafka, json_data
            )
            
            return pb.PosResponse(
                success=True,
                result='Success',
            )
        except asyncio.CancelledError:
            raise  # 이건 gRPC에 그대로 CancelledError로 전달됨
    
        except Exception as e:
            return pb.PosResponse(
                success=False,
                result=''
            )
            
    async def Status(self, request, context):
        try:
            json_data = {
                'robot_id': request.robot_id,
                'battery': float(request.battery),
                'status': request.status,
                'error': request.error,
                'timestamp': datetime.datetime.now().isoformat(),
            }
            await conn_service.status_service(
                self.kafka, json_data
            )
            
            return pb.StatusResponse(
                success=True,
                result='Success',
            )
        except asyncio.CancelledError:
            raise  # 이건 gRPC에 그대로 CancelledError로 전달됨
    
        except Exception as e:
            return pb.StatusResponse(
                success=False,
                result=''
            )


async def serve():
    print("Starting gRPC Robot API Gateway Server...")
    service = RobotGatewayService()
    await service.__aenter__()
    svr = server()
    pb_grpc.add_RobotApiGatewayServicer_to_server(service, svr)
    svr.add_insecure_port("[::]:50051")

    await svr.start()
    print("gRPC Robot API Gateway Server is running...")

    try:
        await svr.wait_for_termination()
    except asyncio.CancelledError:
            # 테스트에서 서버 task를 cancel 할 때를 위한 처리
        await svr.stop(0)
    finally:
        service.__aexit__(None, None, None)
    print("gRPC Robot API Gateway Server stopped.")


if __name__ == "__main__":
    asyncio.run(serve())
