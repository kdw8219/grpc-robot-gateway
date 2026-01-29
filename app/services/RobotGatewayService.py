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
from app.sessions.robot_session import SessionChannel
import grpc
import app.generated.robot_request_control_pb2_grpc as rb_control_pb_grpc
import logging
import base64

class RobotGatewayService(pb_grpc.RobotApiGatewayServicer):
    
    async def __aenter__(self, session_manager:RobotSessionManager, logger: logging.Logger,  kafka_producer=None):
        self.settings = get_settings()
        self.client = httpx.AsyncClient()
        self.kafka = kafka_producer or AIOKafkaProducer(
            bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVER,
            value_serializer=lambda m: json.dumps(m).encode('utf-8'),
        )
        self.session_manager = session_manager
        self.logger = logger
        
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
            
            self.logger.info('login success')
            peer = context.peer()
            host = ""
            if peer.startswith("ipv4:"):
                # ipv4:ip:port -> ip
                host = peer.replace("ipv4:", "").split(":", 1)[0]
            elif peer.startswith("ipv6:"):
                # ipv6:[ip]:port -> ip
                host_port = peer.replace("ipv6:", "")
                if host_port.startswith("["):
                    host = host_port.split("]:", 1)[0].lstrip("[")
                else:
                    host = host_port.rsplit(":", 1)[0]
            else:
                raise RuntimeError(f"unsupported peer: {peer}")

            robot_addr = host
            
            session = await self.session_manager.get_or_create(request.robot_id)
            
            session.robot_addr = robot_addr
            
            self.logger.info('session creation')
            
            return pb.LoginResponse(
                success=True,
                access_token=login_response.json().get('access_token', ''),
                refresh_token=login_response.json().get('refresh_token', ''),
            )
        except Exception as e:
            self.logger.info('Exception occurred...' + str(e))
            return pb.LoginResponse(
                success=False,
                access_token="",
                refresh_token="",
            )
            
    async def Heartbeat(self, request, context):
        if context.cancelled():
            self.logger.warning("Heartbeat cancelled early")
        if context.time_remaining() < 0:
            self.logger.warning("Heartbeat deadline exceeded")
        heartbeat_result = ''
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
            heartbeat_result = heartbeat_response.json().get('result', '')
            
            self.logger.info("Heartbeat robot_id : " + request.robot_id)
            ok = await self.session_manager.update_heartbeat(request.robot_id, SessionChannel.COMMAND) # Timer 처리 로직 개선 필요 --> 개선 완료
            
            return pb.HeartbeatResponse(
                success=ok,
                result=heartbeat_result,
            )
        except asyncio.CancelledError:
            raise  # 이건 gRPC에 그대로 CancelledError로 전달됨
    
        except Exception as e:
            return pb.HeartbeatResponse(
                success=False,
                result=heartbeat_result,
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

    async def MapUpload(self, request, context):
        try:
            data_bytes = bytes(request.data)
            json_data = {
                'robot_id': request.robot_id,
                'width': int(request.width),
                'height': int(request.height),
                'resolution': float(request.resolution),
                'origin_x': float(request.origin_x),
                'origin_y': float(request.origin_y),
                'origin_z': float(request.origin_z),
                'origin_w': float(request.origin_w),
                'data_b64': base64.b64encode(data_bytes).decode('ascii'),
                'timestamp_ms': int(request.timestamp_ms),
                'frame_id': request.frame_id,
                'timestamp': datetime.datetime.now().isoformat(),
            }
            await conn_service.map_service(self.kafka, json_data)
            
            return pb.MapUploadResponse(
                success=True,
                result='Success',
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return pb.MapUploadResponse(
                success=False,
                result=''
            )
