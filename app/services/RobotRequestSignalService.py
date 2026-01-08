import app.generated.robot_request_signal_pb2 as rb_signal_pb
import app.generated.robot_request_signal_pb2_grpc as rb_signal_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager
from app.sessions.robot_session import RobotState, SessionChannel
import grpc
import logging
import asyncio
from google.protobuf.json_format import MessageToDict

log = logging.getLogger(__name__)

"""This service is for signaling between robot and grpc-robot-gateway."""
class RobotRequestSignalService(rb_signal_pb_grpc.RobotSignalServiceServicer):
    async def __aenter__(self, session_manager: RobotSessionManager, queue:asyncio.Queue, async_resp_queue:asyncio.Queue, logger:logging.Logger):
        self.session_manager = session_manager
        self.queue = queue
        self.async_resp_queue = async_resp_queue
        self.logger = logger
        self.internal_timer = 0
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # Nothing to clean up right now; kept for symmetry.
        return

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

        # Signal A (robot → 서버)만 처리. 일단 수신만 하고 응답은 없음.
        session.robot_stream = context
        # 로봇 시그널 스트림 heartbeat를 초기화
        await self.session_manager.update_heartbeat(robot_id, SessionChannel.ROBOT_SIGNAL)
        log.info("OpenSignalStream: robotr stream bound robot_id=%s peer=%s", robot_id, peer)

        drain_task = asyncio.create_task(self.drain_requests(robot_id, request_iterator, peer, context, first))

        try:
            #return을 해버리면 RPC가 종료되어버림... 그래서 함부로 return을 하지 않는다.
            async for out_msg in self.sender(robot_id, session, peer, context, drain_task):
                yield out_msg
        
        finally:
            if not drain_task.done():
                drain_task.cancel()

    # 요청 스트림을 백그라운드에서 drain 하되, RPC가 끝나면 즉시 종료
    async def drain_requests(self, robot_id, request_iterator, peer, context, first):
        log.info("Signal A drain start robot_id=%s", robot_id)
        try:
            async def iterate():
                yield first
                
                # 여기서부터 문제 생김. request_iterator에서 메시지를 async로 가져오게 해놨지만 이 때 UsageError('RPC already finished.') 발생
                # RPC가 이미 끝나 있다는 건데, 이 의미는 서버가 아닌 클라이언트에서 끊었다는 뜻인데..
                async for msg in request_iterator:
                    
                    yield msg
            
            async for msg in iterate(): #get request from robot. I can get message!
                # 채널 heartbeat를 갱신해 세션을 유지한다.
                await self.session_manager.update_heartbeat(robot_id, SessionChannel.ROBOT_SIGNAL)
                #from message queue, get message and send it to robot
                
                payload_type = msg.WhichOneof("payload")
                if not payload_type:
                    continue
                
                log.info("!!!PayloadType! = %s robot_id=%s peer=%s message=%s", payload_type, robot_id, peer, MessageToDict(msg))
                
                payload = {
                    "robot_id": msg.robot_id,
                    "payload_type": payload_type,
                }
                payload[payload_type] = MessageToDict(
                    getattr(msg, payload_type),
                    preserving_proto_field_name=True,
                )
                await self.async_resp_queue.put(payload)
                    
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
                "Signal A drain ended robot_id=%s peer=%s: %s cancelled=%s done = %s code=%s",
                robot_id,
                peer,
                e,
                context.cancelled(),
                context.done(),
                _safe_context_code(context),
            )
        except Exception as e:
            log.warning(
                "Signal A drain error robot_id=%s peer=%s: %r cancelled=%s done = %s code=%s",
                robot_id,
                peer,
                e,
                context.cancelled(),
                context.done(),
                _safe_context_code(context),
            )
        finally:
            log.info(
                "Signal A drain finished robot_id=%s peer=%s cancelled=%s done = %s code=%s",
                robot_id,
                peer,
                context.cancelled(),
                context.done(),
                _safe_context_code(context),
            )        

    async def sender(self, robot_id, session, peer, context, drain_task):
        # 데이터 전달 목적
            try:
                log.info("Signal A response: start robot_id=%s peer=%s", robot_id, peer)
                # 즉시 1회 keepalive/ack 메시지를 내려 스트림을 연다.
                log.info("Signal A response: first keepalive robot_id=%s peer=%s", robot_id, peer)
                yield rb_signal_pb.SignalMessage(robot_id=robot_id)
                
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
                    
                    try:
                        item = await asyncio.wait_for(self.queue.get(), timeout=0.1)
                        self.internal_timer += 0.1
                        msg = self._build_signal_message(item, robot_id)
                        if msg is not None:
                            yield msg
                        
                    except asyncio.TimeoutError:
                        self.internal_timer += 0.1

                    
                    if self.internal_timer >= 10:
                        log.info("Signal A response: keepalive robot_id=%s peer=%s", robot_id, peer)
                        yield rb_signal_pb.SignalMessage(robot_id=robot_id)
                        self.internal_timer = 0    
                    
                    continue
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

    def _build_signal_message(self, item, fallback_robot_id):
        payload_type = item.get("payload_type")
        if not payload_type:
            log.warning("Signal A response: missing payload_type robot_id=%s item=%s", fallback_robot_id, item)
            return None

        robot_id = item.get("robot_id", fallback_robot_id)
        msg = None
        if payload_type == "screen_request":
            msg = rb_signal_pb.SignalMessage(
                robot_id=robot_id,
                screen_request=rb_signal_pb.ScreenRequest(),
            )
        elif payload_type == "robot_offer":
            offer = item["robot_offer"]
            msg = rb_signal_pb.SignalMessage(
                robot_id=robot_id,
                robot_offer=rb_signal_pb.RobotOffer(
                    sdp=offer.get("sdp", ""),
                    type=offer.get("type", ""),
                ),
            )
        elif payload_type == "robot_ice":
            ice = item["robot_ice"]
            msg = rb_signal_pb.SignalMessage(
                robot_id=robot_id,
                robot_ice=rb_signal_pb.IceCandidate(
                    candidate=ice.get("candidate", ""),
                    sdp_mid=ice.get("sdp_mid", ""),
                    sdp_mline_index=ice.get("sdp_mline_index", 0),
                ),
            )
        elif payload_type == "client_answer":
            answer = item["client_answer"]
            msg = rb_signal_pb.SignalMessage(
                robot_id=robot_id,
                client_answer=rb_signal_pb.ClientAnswer(
                    sdp=answer.get("sdp", ""),
                    type=answer.get("type", ""),
                ),
            )
        elif payload_type == "client_ice":
            ice = item["client_ice"]
            msg = rb_signal_pb.SignalMessage(
                robot_id=robot_id,
                client_ice=rb_signal_pb.IceCandidate(
                    candidate=ice.get("candidate", ""),
                    sdp_mid=ice.get("sdp_mid", ""),
                    sdp_mline_index=ice.get("sdp_mline_index", 0),
                ),
            )
        elif payload_type == "webrtc_error":
            err = item["webrtc_error"]
            msg = rb_signal_pb.SignalMessage(
                robot_id=robot_id,
                webrtc_error=rb_signal_pb.WebrtcError(
                    error=err.get("error", ""),
                ),
            )
        elif payload_type == "heartbeat_check":
            msg = rb_signal_pb.SignalMessage(
                robot_id=robot_id,
                heartbeat_check=rb_signal_pb.Heartbeat(),
            )

        if msg is None:
            log.warning(
                "Signal A response: unknown payload_type robot_id=%s payload_type=%s item=%s",
                fallback_robot_id,
                payload_type,
                item,
            )
            return None
        return msg

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
