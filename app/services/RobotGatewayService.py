import app.generated.robot_api_gateway_pb2 as pb
import app.generated.robot_api_gateway_pb2_grpc as pb_grpc
from app.core.config import get_settings
from aiokafka import AIOKafkaProducer
import httpx
import datetime
import json
import app.services.conn_service as conn_service
import asyncio
from app.sessions.robot_session_manager import RobotSessionManager
import grpc
import app.generated.robot_request_control_pb2_grpc as rb_control_pb_grpc

class RobotGatewayService(pb_grpc.RobotApiGatewayServicer):
    
    async def __aenter__(self, session_manager:RobotSessionManager,  kafka_producer=None):
        self.settings = get_settings()
        self.client = httpx.AsyncClient()
        self.kafka = kafka_producer or AIOKafkaProducer(
            bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVER,
            value_serializer=lambda m: json.dumps(m).encode('utf-8'),
        )
        self.session_manager = session_manager
        
        if isinstance(self.kafka, AIOKafkaProducer):   # 실제 producer일 때만 start
            await self.kafka.start()
            
        return self
    
    def recreate_channel(self):
        pass
    
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
            
            print('login success')
            peer = context.peer()
            addr = ""
            if peer.startswith("ipv4:"):
                addr = peer.replace("ipv4:", "")
            elif peer.startswith("ipv6:"):
                host, port = peer.replace("ipv6:", "").rsplit("]:", 1)
                addr = f"{host}:{port}"
            else:
                raise RuntimeError(f"unsupported peer: {peer}")

            robot_addr = addr
            
            session = await self.session_manager.get_or_create(request.robot_id)
            
            print('session creation')
            
            if not session.control_channel:
                channel = grpc.aio.insecure_channel(robot_addr)
                stub = rb_control_pb_grpc.RobotRequestControlServiceStub(channel)

                session.control_channel = channel
                session.control_stub = stub
                session.robot_addr = robot_addr
                
            if session.robot_addr != robot_addr:
                await session.control_channel.close()
                channel = grpc.aio.insecure_channel(robot_addr)
                stub = rb_control_pb_grpc.RobotRequestControlServiceStub(channel)

                session.control_channel = channel
                session.control_stub = stub
                
            print('session creation complete')
            
            return pb.LoginResponse(
                success=True,
                access_token=login_response.json().get('access_token', ''),
                refresh_token=login_response.json().get('refresh_token', ''),
            )
        except Exception as e:
            print('Exception occurred...' + str(e))
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
            
            ok = await self.session_manager.update_heartbeat(request.robot_id)
            
            return pb.HeartbeatResponse(
                success=ok,
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
                'linear_speed': float(request.linear_speed),
                'angular_speed': float(request.angular_speed),
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

