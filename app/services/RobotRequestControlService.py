import app.generated.robot_request_control_pb2 as rb_control_pb
import app.generated.robot_request_control_pb2_grpc as rb_control_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager
from app.sessions.robot_session import RobotState
import grpc
from logging import Logger
import asyncio

class RobotRequestControlService(rb_control_pb_grpc.RobotRequestControlServiceServicer):
    
    async def __aenter__(self, session_manager:RobotSessionManager, queue:asyncio.Queue, async_resp_queue:asyncio.Queue, logger:Logger):
        self.session_manager = session_manager
        self.queue = queue
        self.async_resp_queue = async_resp_queue
        self.logger = logger
        return self
        
    
    async def __aexit__(self, exc_type, exc, tb):
        self.executor.shutdown(wait=False)
       
    
    async def GetNextCommand(self, request, context):
        session = await self.session_manager.get(request.robot_id)
        
        #get data from RobotControlService
        
        if not session:
            return rb_control_pb.RobotCommandResponse(
                has_command=False,
            )
        if session.state != RobotState.CONNECTED:
            return rb_control_pb.RobotCommandResponse(
                has_command=False,
            )

        try:
            item = await asyncio.wait_for(self.queue.get(), timeout=0.1)
            
            payload_type = item.get("command")
            
            print("get item:", item)
            
            if payload_type == "MOVE":
                return rb_control_pb.RobotCommandResponse(
                    has_command = True,
                    command = item.get('command'),
                    move = item.get('move')
                )

            elif payload_type == "SET_SPEED":
                return rb_control_pb.RobotCommandResponse(
                    has_command = True,
                    command = item.get('command'),
                    set_speed = item.get('set_speed')
                )

            elif payload_type == "PATH_FOLLOW":
                return rb_control_pb.RobotCommandResponse(
                    has_command = True,
                    command = item.get('command'),
                    path_follow = item.get('path_follow')
                )
                
            return rb_control_pb.RobotCommandResponse(
                has_command = True,
                command = item.get('command'),
            )   
        except asyncio.TimeoutError:
            self.logger.debug('no data from queue')
            return rb_control_pb.RobotCommandResponse(
                has_command=False,
            )
        except grpc.aio.AioRpcError as e:
            await self.session_manager.mark_offline_and_cleanup(session, request.robot_id)

            return rb_control_pb.RobotCommandResponse(
                has_command=False,
                command = rb_control_pb.RobotCommand.Value("CMD_NONE"),
            )    
