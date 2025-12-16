import app.generated.control_pb2 as control_pb
import app.generated.control_pb2_grpc as control_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager

class RobotControlService(control_pb_grpc.RobotControlServiceServicer):
    
    async def __aenter__(self, session_manager:RobotSessionManager):
        self.session_manager = session_manager
        return self
        
    
    async def __aexit__(self, exc_type, exc, tb):
        self.executor.shutdown(wait=False)
       
    
    async def Command(self, request, context):
        
        try:
            control_pb.CommandRequest()
            
            return control_pb.CommandResponse(
                success=True,
            )
        except Exception:
            return control_pb.CommandResponse(
                success=False,
                access_token="",
                refresh_token="",
            )
            

