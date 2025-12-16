import app.generated.signaling_pb2 as signaling_pb
import app.generated.signaling_pb2_grpc as signaling_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager

class RobotSignalService(signaling_pb_grpc.RobotSignalServiceServicer):
    
    async def __aenter__(self, session_manager:RobotSessionManager):
        self.session_manager = session_manager    
        return self
        
    
    async def __aexit__(self, exc_type, exc, tb):
        self.executor.shutdown(wait=False)
        
    
    async def Screen(self, request, context):
        try:
            signaling_pb.ScreenRequest()
            
            return signaling_pb.ScreenResponse(
                success=True,
            )
        except Exception:
            return signaling_pb.ScreenResponse(
                success=False,
            )
            