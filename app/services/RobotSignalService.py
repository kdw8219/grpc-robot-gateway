import app.generated.signaling_pb2 as signaling_pb
import app.generated.signaling_pb2_grpc as signaling_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager
import asyncio
import logging

log = logging.getLogger(__name__)

class RobotSignalService(signaling_pb_grpc.RobotSignalServiceServicer):
    
    async def __aenter__(self, session_manager:RobotSessionManager):
        self.session_manager = session_manager    
        return self
        
    
    async def __aexit__(self, exc_type, exc, tb):
        # Nothing to clean up right now; kept for symmetry.
        return
        
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

        # Signal A (gateway → 서버)만 처리. 일단 수신만 하고 응답은 없음.
        session.gateway_stream = context

        async def consume():
            # 첫 메시지 포함해서 모두 소비 (현재는 저장/변환 없이 드랍)
            log.debug("Signal A received first msg for robot_id=%s", robot_id)
            async def iterate():
                yield first
                async for msg in request_iterator:
                    yield msg
 
            async for msg in iterate():
                log.debug("Signal A msg robot_id=%s payload_set=%s", robot_id, msg.WhichOneof("payload"))

        consume_task = asyncio.create_task(consume())

        async def empty_stream():
            try:
                await consume_task
            except asyncio.CancelledError:
                consume_task.cancel()
                raise
            # 응답 없음
            if False:
                yield None

        return empty_stream()
