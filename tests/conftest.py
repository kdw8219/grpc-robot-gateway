import asyncio
import pytest
import grpc
from grpc.experimental import aio

from app.server.main import RobotGatewayService
from app.generated import robot_api_gateway_pb2_grpc as pb2_grpc

@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
    
    
    
@pytest.fixture(scope='function')
async def grpc_server():
    server = aio.server()
    pb2_grpc.add_RobotApiGatewayServicer_to_server(RobotGatewayService(), server)
    
    listen_addr = 'localhost:50051'
    port = server.add_insecure_port(listen_addr)
    await server.start()
    yield f'localhost:{port}'
    await server.stop(0)