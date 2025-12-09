import asyncio
import pytest
import grpc
from grpc.experimental import aio

from app.server.main import RobotGatewayService
from app.generated import robot_api_gateway_pb2_grpc as pb2_grpc

from unittest.mock import AsyncMock

@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
    
    
    
@pytest.fixture(scope='function')
async def grpc_server():
    mock_kafka = AsyncMock()
    
    service = RobotGatewayService()
    server = aio.server()
    
    service.__aenter__(kafka_producer=mock_kafka)
    pb2_grpc.add_RobotApiGatewayServicer_to_server(service, server)
        
    listen_addr = 'localhost:50051'
    port = server.add_insecure_port(listen_addr)
    await server.start()
    yield f'localhost:{port}'
    await server.stop(0)
    await service.__aexit__(None, None, None)