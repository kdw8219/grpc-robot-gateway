import asyncio
import logging
import grpc

import app.generated.signaling_pb2 as signaling_pb
import app.generated.signaling_pb2_grpc as signaling_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager
from app.sessions.robot_session import RobotState
from google.protobuf.json_format import MessageToDict

log = logging.getLogger(__name__)

'''This service is for signaling between robot gateway and robot server.'''
class RobotSignalService(signaling_pb_grpc.RobotSignalServiceServicer):
    async def __aenter__(self, session_manager: RobotSessionManager):
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

        def _on_done(_):
            try:
                log.warning(
                    "OpenSignalStream done: peer=%s cancelled=%s code=%s details=%s",
                    peer,
                    context.cancelled(),
                    _safe_context_code(context),
                    _safe_context_details(context),
                )
            except Exception as e:
                log.warning("OpenSignalStream done logging failed for peer=%s: %r", peer, e)

        context.add_done_callback(_on_done)

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
                log.warning(
                    "OpenSignalStream: still waiting for first message (peer=%s, elapsed=%.1fs)",
                    peer,
                    elapsed,
                )
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

        drain_task = asyncio.create_task(self.drain_requests(robot_id, request_iterator, peer, context, first))

        return self.receiver(robot_id, session, peer, context, drain_task)

    # 요청 스트림을 백그라운드에서 drain 하되, RPC가 끝나면 즉시 종료
    async def drain_requests(self, robot_id, request_iterator, peer, context, first):
        log.info("Signal A drain start robot_id=%s", robot_id)
        try:
            async def iterate():
                yield first
                async for msg in request_iterator:
                    yield msg

            async for msg in iterate():
                command_type =msg.WhichOneof("payload")
                log.info("Signal A msg robot_id=%s payload_set=%s", robot_id, command_type)
                # after msg done, send it to robot session
                # control / signal 이 2가지가 들어오게 되므로 이를 구분하도록 구현 필요
                
                if command_type == "control_command":
                    data= self.control_to_minimal_dict(msg)
                    if data is None:
                        log.warning("Signal A drain: unknown control command robot_id=%s peer=%s", robot_id, peer)
                        continue
                    
                    # send to robot session
                    
                else:
                    data= self.signal_to_webrtc_dict(msg)
                    if data is None:
                        log.warning("Signal A drain: unknown signal command robot_id=%s peer=%s", robot_id, peer)
                        continue
                    
                    # send to robot session
                
                
                    
        except StopAsyncIteration:
            log.info(
                "Signal A drain iterator closed robot_id=%s peer=%s cancelled=%s code=%s",
                robot_id,
                peer,
                context.cancelled(),
                _safe_context_code(context),
            )
        except grpc.RpcError as e:
            log.warning(
                "Signal A drain ended robot_id=%s peer=%s: %s cancelled=%s code=%s",
                robot_id,
                peer,
                e,
                context.cancelled(),
                _safe_context_code(context),
            )
        except Exception as e:
            log.warning(
                "Signal A drain error robot_id=%s peer=%s: %r cancelled=%s code=%s",
                robot_id,
                peer,
                e,
                context.cancelled(),
                _safe_context_code(context),
            )
        finally:
            log.info(
                "Signal A drain finished robot_id=%s peer=%s cancelled=%s code=%s",
                robot_id,
                peer,
                context.cancelled(),
                _safe_context_code(context),
            )        

    def control_to_minimal_dict(self, msg):
        if msg.WhichOneof("payload") != "control_command":
            return None
        cc = msg.control_command
        payload_type = cc.WhichOneof("payload")  # "move" | "set_speed" | "path_follow" | None

        payload = {
            "robot_id": msg.robot_id,
            "command": cc.command,  # enum number; 필요하면 Name(...)으로 문자열화
        }
        if payload_type:
            payload[payload_type] = MessageToDict(
                getattr(cc, payload_type),
                preserving_proto_field_name=True,
            )
        return payload
    
    def signal_to_webrtc_dict(self, msg):
        payload_type = msg.WhichOneof("payload")
        if payload_type is None or payload_type == "control_command":
            return None 

        data = {"robot_id": msg.robot_id, "payload_type": payload_type}
        data[payload_type] = MessageToDict(
            getattr(msg, payload_type),
            preserving_proto_field_name=True,
        )
        return data

    async def receiver(self, robot_id, session, peer, context, drain_task):
        # 응답 스트림: 이 함수 자체를 async generator로 만들어 RPC를 붙잡는다.
            try:
                log.info("Signal A response: start robot_id=%s peer=%s", robot_id, peer)
                # 즉시 1회 keepalive/ack 메시지를 내려 스트림을 연다.
                log.info("Signal A response: first keepalive robot_id=%s peer=%s", robot_id, peer)
                yield signaling_pb.SignalMessage(robot_id=robot_id)
                while True:
                    if context.cancelled():
                        log.info(
                            "Signal A response: context cancelled robot_id=%s peer=%s code=%s",
                            robot_id,
                            peer,
                            _safe_context_code(context),
                        )
                        break
                    if context.done():
                        log.info(
                            "Signal A response: context done robot_id=%s peer=%s code=%s",
                            robot_id,
                            peer,
                            _safe_context_code(context),
                        )
                        break
                    if session.state != RobotState.CONNECTED:
                        log.info(
                            "Signal A response: session offline robot_id=%s peer=%s state=%s",
                            robot_id,
                            peer,
                            session.state,
                        )
                        break
                    log.info("Signal A response: keepalive robot_id=%s peer=%s", robot_id, peer)
                    await asyncio.sleep(10)
                    yield signaling_pb.SignalMessage(robot_id=robot_id)
            except asyncio.CancelledError:
                log.info("Signal A response cancelled robot_id=%s peer=%s", robot_id, peer)
                raise
            except Exception as e:
                log.warning(
                    "Signal A response error robot_id=%s peer=%s: %r cancelled=%s code=%s",
                    robot_id,
                    peer,
                    e,
                    context.cancelled(),
                    _safe_context_code(context),
                )
            finally:
                if not drain_task.done():
                    drain_task.cancel()
                try:
                    await drain_task
                except Exception:
                    pass
                log.info(
                    "Signal A response finished robot_id=%s peer=%s cancelled=%s code=%s",
                    robot_id,
                    peer,
                    context.cancelled(),
                    _safe_context_code(context),
                )

def _safe_context_code(context):
    try:
        return context.code()
    except Exception:
        return None


def _safe_context_details(context):
    try:
        return context.details()
    except Exception:
        return None
