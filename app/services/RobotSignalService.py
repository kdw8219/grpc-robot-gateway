import app.generated.signaling_pb2 as signaling_pb
import app.generated.signaling_pb2_grpc as signaling_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager
import asyncio
import logging
import grpc

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
        peer = context.peer()
        log.info("OpenSignalStream: peer=%s connected", peer)

        # 1. 첫 메시지에서 robot_id 판별 (최대 60초 대기, 5초마다 경고)
        first = None
        start = asyncio.get_event_loop().time()
        while True:
            try:
                first = await asyncio.wait_for(request_iterator.__anext__(), timeout=5.0)
                break
            except asyncio.TimeoutError:
                elapsed = asyncio.get_event_loop().time() - start
                if elapsed >= 60.0:
                    log.warning("OpenSignalStream: no first message for 60s (peer=%s), closing", peer)
                    return
                log.warning("OpenSignalStream: still waiting for first message (peer=%s, elapsed=%.1fs)", peer, elapsed)
                continue
            except StopAsyncIteration:
                # 클라이언트가 첫 메시지 없이 스트림을 닫은 경우
                log.warning("OpenSignalStream: stream closed before first message (peer=%s)", peer)
                return
        robot_id = first.robot_id or ""

        if not robot_id:
            log.warning("OpenSignalStream: missing robot_id in first message (peer=%s)", peer)
            return

        session = await self.session_manager.get_or_create(robot_id)

        # Signal A (gateway → 서버)만 처리. 일단 수신만 하고 응답은 없음.
        session.gateway_stream = context
        log.info("OpenSignalStream: gateway stream bound robot_id=%s peer=%s", robot_id, peer)

        async def consume():
            log.debug("Signal A received first msg for robot_id=%s", robot_id)
            try:
                # 첫 메시지 포함 전체를 순회 (현재는 저장/변환 없이 드랍)
                async def iterate():
                    yield first
                    async for msg in request_iterator:
                        yield msg

                async for msg in iterate():
                    log.debug("Signal A msg robot_id=%s payload_set=%s", robot_id, msg.WhichOneof("payload"))
            except grpc.RpcError as e:
                log.warning("Signal A consume ended robot_id=%s peer=%s: %s", robot_id, peer, e)
            except Exception as e:
                log.warning("Signal A consume error robot_id=%s peer=%s: %r", robot_id, peer, e)
            finally:
                log.info("Signal A consume finished robot_id=%s peer=%s", robot_id, peer)

        consume_task = asyncio.create_task(consume())

        async def response_stream():
            try:
                # 즉시 1회 keepalive/ack 메시지를 내려 스트림을 연다.
                yield signaling_pb.SignalMessage(robot_id=robot_id)
                while True:
                    if context.cancelled():
                        log.info("Signal A response: context cancelled robot_id=%s peer=%s", robot_id, peer)
                        break
                    await asyncio.sleep(10)
                    yield signaling_pb.SignalMessage(robot_id=robot_id)
            except asyncio.CancelledError:
                log.info("Signal A response cancelled robot_id=%s peer=%s", robot_id, peer)
                consume_task.cancel()
                raise
            finally:
                if not consume_task.done():
                    consume_task.cancel()
                try:
                    await consume_task
                except Exception:
                    pass
                code = None
                try:
                    code = context.code()
                except Exception:
                    pass
                log.info(
                    "Signal A response finished robot_id=%s peer=%s cancelled=%s code=%s",
                    robot_id,
                    peer,
                    context.cancelled(),
                    code,
                )

        return response_stream()
