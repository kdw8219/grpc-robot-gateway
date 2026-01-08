
import os
import sys
import logging
import asyncio

# os.environ.setdefault("GRPC_VERBOSITY", "DEBUG")
# os.environ.setdefault("GRPC_TRACE", "tcp,http,secure_endpoint,transport_security")

from grpc.aio import server

import app.generated.robot_api_gateway_pb2_grpc as pb_grpc
import app.generated.signaling_pb2_grpc as signaling_pb_grpc
import app.generated.robot_request_control_pb2_grpc as rb_control_pb_grpc
import app.generated.robot_request_signal_pb2_grpc as rb_signal_pb_grpc

import app.services.RobotGatewayService as RobotGatewayService
import app.services.RobotSignalService as RobotSignalService
import app.services.RobotRequestControlService as RobotRequestControlService
import app.services.RobotRequestSignalService as RobotRequestSignalService

from app.sessions.robot_session_manager import RobotSessionManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

async def start_session_watcher(manager: RobotSessionManager):
    try:
        while True:
            await manager.sweep_expired()
            await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        # shutdown 시 정리 작업
        logger.info("Session watcher cancelled")
        raise

async def serve():
    print("Starting gRPC Robot API Gateway Server...")
    
    session_manager = RobotSessionManager(logger)
    async_controller = asyncio.create_task(start_session_watcher(session_manager))
    
    service = RobotGatewayService.RobotGatewayService()
    signal_service = RobotSignalService.RobotSignalService()
    robot_control_service = RobotRequestControlService.RobotRequestControlService()
    robot_signal_service = RobotRequestSignalService.RobotRequestSignalService()
    
    command_to_robot_command = asyncio.Queue()
    signal_to_robot_command = asyncio.Queue()
    response_queue = asyncio.Queue()
    
    await service.__aenter__(session_manager, logger)
    await signal_service.__aenter__(session_manager, command_to_robot_command, signal_to_robot_command, response_queue)
    await robot_control_service.__aenter__(session_manager, command_to_robot_command, response_queue, logger)
    await robot_signal_service.__aenter__(session_manager, signal_to_robot_command, response_queue, logger)
    
    svr = server()
    pb_grpc.add_RobotApiGatewayServicer_to_server(service, svr) # heartbeat, login, status, pos
    signaling_pb_grpc.add_RobotSignalServiceServicer_to_server(signal_service, svr) # signaling(screen) from server
    
    rb_signal_pb_grpc.add_RobotSignalServiceServicer_to_server(robot_signal_service, svr)
    rb_control_pb_grpc.add_RobotRequestControlServiceServicer_to_server(robot_control_service, svr)
    
    svr.add_insecure_port("[::]:50051")

    await svr.start()
    print("gRPC Robot API Gateway Server is running...")
    
    try:
        await svr.wait_for_termination()
    except asyncio.CancelledError:
            # 테스트에서 서버 task를 cancel 할 때를 위한 처리
        await svr.stop(0)
    finally:
        await service.__aexit__(None, None, None)
        await signal_service.__aexit__(None, None, None)
        await robot_control_service.__aexit__(None,None,None)
        await robot_signal_service.__aexit__(None,None,None)
        async_controller.cancel()
        
    print("gRPC Robot API Gateway Server stopped.")


if __name__ == "__main__":
    asyncio.run(serve())
