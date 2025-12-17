import app.generated.robot_request_control_pb2 as rb_control_pb
import app.generated.robot_request_control_pb2_grpc as rb_control_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager
from app.sessions.robot_session import RobotState
import grpc
import queue

class RobotRequestControlService(rb_control_pb_grpc.RobotRequestControlServiceServicer):
    
    async def __aenter__(self, session_manager:RobotSessionManager, queue:queue.Queue):
        self.session_manager = session_manager
        self.queue = queue
        return self
        
    
    async def __aexit__(self, exc_type, exc, tb):
        self.executor.shutdown(wait=False)
       
    
    async def GetNextCommand(self, request, context):
        session = await self.session_manager.get(request.robot_id)
        
        #get data from RobotControlService
        
        if not session:
            return rb_control_pb_grpc.CommandResponse(
                success=False,
                message="robot session not found"
            )
        if session.state != RobotState.CONNECTED:
            return rb_control_pb_grpc.CommandResponse(
                success=False,
                message="robot offline"
            )

        try:
            item = self.queue.get()
            
            payload_type = item.WhichOneof("payload")
            
            if payload_type == "move":
                self.queue.task_done()  
                return rb_control_pb.RobotCommandResponse(
                    has_command = True,
                    command = item.command,
                    move = item.move
                )

            elif payload_type == "set_speed":
                self.queue.task_done()  
                return rb_control_pb.RobotCommandResponse(
                    has_command = True,
                    command = item.command,
                    set_speed = item.set_speed
                )

            elif payload_type == "path_follow":
                self.queue.task_done()  
                return rb_control_pb.RobotCommandResponse(
                    has_command = True,
                    command = item.command,
                    path_follow = item.path_follow
                )
                
            self.queue.task_done()  
            return rb_control_pb_grpc.CommandResponse(
                has_command = True,
                command = item.command,
            )   
            
        except grpc.aio.AioRpcError as e:
            await self.session_manager.mark_offline_and_cleanup(session, request.robot_id)

            return rb_control_pb_grpc.CommandResponse(
                success=False,
                message=f"robot unreachable: {e.details()}"
            )    

