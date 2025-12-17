import app.generated.control_pb2 as control_pb
import app.generated.control_pb2_grpc as control_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager
from app.sessions.robot_session import RobotState
import grpc
import queue

class RobotControlService(control_pb_grpc.RobotControlServiceServicer):
    
    async def __aenter__(self, session_manager:RobotSessionManager, queue:queue.Queue):
        self.session_manager = session_manager
        self.queue = queue
        return self
        
    
    async def __aexit__(self, exc_type, exc, tb):
        self.executor.shutdown(wait=False)
       
    
    async def SendCommand(self, request, context):
        session = await self.session_manager.get(request.robot_id)
        
        if not session:
            return control_pb_grpc.CommandResponse(
                success=False,
                message="robot session not found"
            )
        if session.state != RobotState.CONNECTED:
            return control_pb_grpc.CommandResponse(
                success=False,
                message="robot offline"
            )
       
        try:
            #using request and convert it to something else
            data = request.SerializeToString()
            self.queue.put(data)
            
            return control_pb.CommandResponse(
                success=True,
                message = "robot request success"
            )
        except grpc.aio.AioRpcError as e:
            await self.session_manager.mark_offline_and_cleanup(session, request.robot_id)

            return control_pb.CommandResponse(
                success=False,
                message=f"robot unreachable: {e.details()}"
            )    

