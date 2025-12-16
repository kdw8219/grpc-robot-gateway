
from grpc.aio import server
import asyncio

import app.generated.robot_api_gateway_pb2_grpc as pb_grpc
import app.generated.control_pb2_grpc as control_pb_grpc
import app.generated.signaling_pb2_grpc as signaling_pb_grpc
import app.services.RobotGatewayService as RobotGatewayService
import app.services.RobotControlService as RobotControlService
import app.services.RobotSignalService as RobotSignalService
from app.sessions.robot_session_manager import RobotSessionManager

async def start_session_watcher(manager: RobotSessionManager):
    try:
        while True:
            await manager.sweep_expired()
            await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        # shutdown 시 정리 작업
        print("Session watcher cancelled")
        raise

async def serve():
    print("Starting gRPC Robot API Gateway Server...")
    
    session_manager = RobotSessionManager()
    async_controller = asyncio.create_task(start_session_watcher(session_manager))
    
    service = RobotGatewayService.RobotGatewayService()
    control_service = RobotControlService.RobotControlService()
    signal_service = RobotSignalService.RobotSignalService()
    
    await service.__aenter__(session_manager)
    await control_service.__aenter__(session_manager)
    await signal_service.__aenter__(session_manager)
    
    svr = server()
    pb_grpc.add_RobotApiGatewayServicer_to_server(service, svr) # heartbeat, login, status, pos
    control_pb_grpc.add_RobotControlServiceServicer_to_server(control_service, svr) # command(control)
    signaling_pb_grpc.add_RobotSignalServiceServicer_to_server(signal_service, svr) # signaling(screen)
    svr.add_insecure_port("[::]:50051")

    await svr.start()
    print("gRPC Robot API Gateway Server is running...")
    
    try:
        await svr.wait_for_termination()
    except asyncio.CancelledError:
            # 테스트에서 서버 task를 cancel 할 때를 위한 처리
        await svr.stop(0)
    finally:
        service.__aexit__(None, None, None)
        control_service.__aexit__(None, None, None)
        signal_service.__aexit__(None, None, None)
        async_controller.cancel()
        await async_controller
    print("gRPC Robot API Gateway Server stopped.")


if __name__ == "__main__":
    asyncio.run(serve())
