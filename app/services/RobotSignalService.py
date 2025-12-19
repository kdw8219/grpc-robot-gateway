import app.generated.signaling_pb2 as signaling_pb
import app.generated.signaling_pb2_grpc as signaling_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager
import grpc
import logging

log = logging.getLogger(__name__)

class RobotSignalService(signaling_pb_grpc.RobotSignalServiceServicer):
    
    async def __aenter__(self, session_manager:RobotSessionManager):
        self.session_manager = session_manager    
        return self
        
    
    async def __aexit__(self, exc_type, exc, tb):
        # Nothing to clean up right now; kept for symmetry.
        return
    
    async def relay(self, session, msg, from_robot: bool):
        target = (
            session.gateway_stream if from_robot
            else session.robot_stream
        )

        if not target:
            return  

        await target.write(msg)
        
    # 추후에 meta data 기반으로 구분할 수 있게 수정 필요
    # 현재는 robot에서 보내는 meta data에 데이터를 넣게 되어 있진 않다.
    def _peer_host(self, peer: str) -> str:
        if peer.startswith("ipv4:"):
            # ipv4:ip:port -> ip
            return peer.replace("ipv4:", "").split(":", 1)[0]
        if peer.startswith("ipv6:"):
            # ipv6:[ip]:port -> ip
            host_port = peer.replace("ipv6:", "")
            if host_port.startswith("["):
                return host_port.split("]:", 1)[0].lstrip("[")
            return host_port.rsplit(":", 1)[0]
        return ""

    def is_robot_peer(self, peer, session):
        peer_host = self._peer_host(peer)
        # Compare only host (not port), since login and signaling streams use different ports.
        return session.robot_addr == peer_host
    
    async def OpenSignalStream(self, request_iterator, context):
        # 1. 첫 메시지에서 robot_id 판별
        try:
            first = await request_iterator.__anext__()
        except StopAsyncIteration:
            # 클라이언트가 첫 메시지 없이 스트림을 닫은 경우
            log.warning("OpenSignalStream: stream closed before first message (peer=%s)", context.peer())
            return
        robot_id = first.robot_id

        session = await self.session_manager.get_or_create(robot_id)

        # 2. caller 구분
        peer = context.peer()
        is_robot = self.is_robot_peer(peer, session)

        if is_robot:
            session.robot_stream = context
        else:
            session.gateway_stream = context

        await self.relay(session, first, from_robot=is_robot)

        async for msg in request_iterator:
            await self.relay(session, msg, from_robot=is_robot)
            
