import app.generated.control_pb2 as control_pb
import app.generated.control_pb2_grpc as control_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager
from app.sessions.robot_session import RobotState
import grpc

class RobotControlService(control_pb_grpc.RobotControlServiceServicer):
    
    async def __aenter__(self, session_manager:RobotSessionManager):
        self.session_manager = session_manager
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

        if not session.control_stub:
            return control_pb_grpc.CommandResponse(
                success=False,
                message="control channel not ready"
            )
            
        try:
            await session.control_stub.SendCommand(request)
                
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

