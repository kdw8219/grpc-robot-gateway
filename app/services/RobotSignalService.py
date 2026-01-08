import asyncio
import logging
import grpc

import app.generated.signaling_pb2 as signaling_pb
import app.generated.signaling_pb2_grpc as signaling_pb_grpc
from app.sessions.robot_session_manager import RobotSessionManager
from app.sessions.robot_session import RobotState
from google.protobuf.json_format import MessageToDict
from app.sessions.robot_session import RobotState, SessionChannel
from queue import Queue
from asyncio import Queue as AsyncQueue

log = logging.getLogger(__name__)

'''This service is for signaling between robot gateway and robot server.'''
class RobotSignalService(signaling_pb_grpc.RobotSignalServiceServicer):
    async def __aenter__(self, session_manager: RobotSessionManager, control_queue:Queue, signal_queue:Queue, async_resp_queue:AsyncQueue):
        self.session_manager = session_manager
        self.control_queue = control_queue
        self.signal_queue = signal_queue
        self.response_queue = async_resp_queue
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

        # Signal A (gateway → 서버)만 처리. 일단 수신만 하고 응답은 없음.
        session.gateway_stream = context
        log.info("OpenSignalStream: gateway stream bound robot_id=%s peer=%s", robot_id, peer)

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
                async for msg in request_iterator:
                    yield msg

            await self.session_manager.update_heartbeat(robot_id, SessionChannel.SERVER_SIGNAL)

            async for msg in iterate():
                command_type =msg.WhichOneof("payload")
                log.info("Signal A msg robot_id=%s payload_set=%s", robot_id, command_type)
                # after msg done, send it to robot session
                # control / signal 이 2가지가 들어오게 되므로 이를 구분하도록 구현 필요
                
                await self.session_manager.update_heartbeat(robot_id, SessionChannel.SERVER_SIGNAL)
                
                if command_type == "control_command":
                    data= self.control_to_minimal_dict(msg)
                    if data is None:
                        log.warning("Signal A drain: unknown control command robot_id=%s peer=%s", robot_id, peer)
                        continue
                    self.control_queue.put(data)
                    log.info("Receive Request and set it to control queue: robot_id=%s peer=%s data=% s", robot_id, peer, data)                
                   
                    # send to robot session
                elif command_type is None or command_type == "heartbeat_check":
                    log.warning("Signal A drain: empty command robot_id=%s peer=%s", robot_id, peer)
                    continue
                
                else:
                    data= self.signal_to_webrtc_dict(msg)
                    if data is None:
                        log.warning("Signal A drain: unknown signal command robot_id=%s peer=%s", robot_id, peer)
                        continue
                    
                    if not await self.session_manager.is_session_alive(robot_id, SessionChannel.ROBOT_SIGNAL): #No robot session...
                        log.warning("robot is not working right now... id : %s peer=%s", robot_id, peer)
                        
                        payload = {
                            "robot_id": robot_id,
                            "payload_type": "error_message",
                            "error_message": {
                                "error_code": 1,
                                "error_description": "Robot is not connected right now."
                            }
                        }
                        await self.response_queue.put(payload)
                        continue
                    
                    self.signal_queue.put(data)
                    log.info("Receive Request and set it to signal queue: robot_id=%s peer=%s data=% s", robot_id, peer, data)
                
                    
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
        
        log.info("signal message from server =%s", MessageToDict(msg, preserving_proto_field_name=True))
        
        if payload_type is None or payload_type == "control_command":
            return None 

        data = {"robot_id": msg.robot_id, "payload_type": payload_type}
        data[payload_type] = MessageToDict(
            getattr(msg, payload_type),
            preserving_proto_field_name=True,
        )
        return data

    async def process_response_queue(self, item):
        payload_type = item.get("payload_type")
        if payload_type == "error_message":
            log.info("Error message received: %s", item)
            return signaling_pb.SignalMessage(
                robot_id=item["robot_id"],
                webrtc_error=signaling_pb.WebrtcError(
                    error=item["error_message"]["error_description"],
                ),
            )

        msg = None
        if payload_type == "screen_request":
            msg = signaling_pb.SignalMessage(
                robot_id=item["robot_id"],
                screen_request=signaling_pb.ScreenRequest(),
            )
        elif payload_type == "robot_offer":
            offer = item["robot_offer"]
            msg = signaling_pb.SignalMessage(
                robot_id=item["robot_id"],
                robot_offer=signaling_pb.RobotOffer(
                    sdp=offer.get("sdp", ""),
                    type=offer.get("type", ""),
                ),
            )
        elif payload_type == "robot_ice":
            ice = item["robot_ice"]
            msg = signaling_pb.SignalMessage(
                robot_id=item["robot_id"],
                robot_ice=signaling_pb.IceCandidate(
                    candidate=ice.get("candidate", ""),
                    sdp_mid=ice.get("sdp_mid", ""),
                    sdp_mline_index=ice.get("sdp_mline_index", 0),
                ),
            )
        elif payload_type == "client_answer":
            answer = item["client_answer"]
            msg = signaling_pb.SignalMessage(
                robot_id=item["robot_id"],
                client_answer=signaling_pb.ClientAnswer(
                    sdp=answer.get("sdp", ""),
                    type=answer.get("type", ""),
                ),
            )
        elif payload_type == "client_ice":
            ice = item["client_ice"]
            msg = signaling_pb.SignalMessage(
                robot_id=item["robot_id"],
                client_ice=signaling_pb.IceCandidate(
                    candidate=ice.get("candidate", ""),
                    sdp_mid=ice.get("sdp_mid", ""),
                    sdp_mline_index=ice.get("sdp_mline_index", 0),
                ),
            )
        elif payload_type == "webrtc_error":
            err = item["webrtc_error"]
            msg = signaling_pb.SignalMessage(
                robot_id=item["robot_id"],
                webrtc_error=signaling_pb.WebrtcError(
                    error=err.get("error", ""),
                ),
            )
        elif payload_type == "heartbeat_check":
            msg = signaling_pb.SignalMessage(
                robot_id=item["robot_id"],
                heartbeat_check=signaling_pb.Heartbeat(),
            )

        if msg is None:
            log.warning(
                "Signal A response: unknown payload_type robot_id=%s payload_type=%s item=%s",
                item.get("robot_id"),
                payload_type,
                item,
            )
        return msg
#just for heartbeating
    async def sender(self, robot_id, session, peer, context, drain_task):
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
                    
                    is_success = False
                    try:
                        item = self.response_queue.get_nowait() #join 할 필요가 없으니까 tasK_done 안함
                        is_success = True
                        self.internal_timer += 0.1
                        
                        log.info("Response to Server!")
                        
                        msg = await self.process_response_queue(item)
                        if msg is not None:
                            yield msg
                        
                    except asyncio.QueueEmpty:
                        self.internal_timer += 0.1
                    finally:
                        if is_success:
                            self.response_queue.task_done()
                            is_success = False
                        else:
                            await asyncio.sleep(0.1)

                    
                    if self.internal_timer >= 10.0:
                        log.info("Signal A response: keepalive robot_id=%s peer=%s", robot_id, peer)
                        yield signaling_pb.SignalMessage(robot_id=robot_id)
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
