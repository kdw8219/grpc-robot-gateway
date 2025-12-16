import app.generated.signaling_pb2 as signaling_pb
import app.generated.signaling_pb2_grpc as signaling_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager
import grpc

class RobotSignalService(signaling_pb_grpc.RobotSignalServiceServicer):
    
    async def __aenter__(self, session_manager:RobotSessionManager):
        self.session_manager = session_manager    
        return self
        
    
    async def __aexit__(self, exc_type, exc, tb):
        self.executor.shutdown(wait=False)
    
    async def relay(session, msg, from_robot: bool):
        target = (
            session.gateway_stream if from_robot
            else session.robot_stream
        )

        if not target:
            return  

        await target.write(msg)
        
    # 추후에 meta data 기반으로 구분할 수 있게 수정 필요
    # 현재는 robot에서 보내는 meta data에 데이터를 넣게 되어 있진 않다.
    def is_robot_peer(peer, session):
        addr = 0
        if peer.startswith("ipv4:"):
            addr = peer.replace("ipv4:", "")
        elif peer.startswith("ipv6:"):
            host, port = peer.replace("ipv6:", "").rsplit("]:", 1)
            addr = f"{host}:{port}"
            
        if session.robot_addr != addr:
            return False
        
        return True
    
    async def OpenSignalStream(self, request_iterator, context):
        # 1. 첫 메시지에서 robot_id 판별
        first = await request_iterator.__anext__()
        robot_id = first.robot_id

        session = await self.session_manager.get(robot_id)
        if not session:
            context.abort(grpc.StatusCode.NOT_FOUND, "session not found")

        # 2. caller 구분
        peer = context.peer()
        is_robot = self.is_robot_peer(peer)

        if is_robot:
            session.robot_stream = context
        else:
            session.gateway_stream = context

        await self.relay(session, first, from_robot=is_robot)

        async for msg in request_iterator:
            await self.relay(session, msg, from_robot=is_robot)
            