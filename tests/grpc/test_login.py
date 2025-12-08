import pytest
import grpc
from grpc.experimental import aio

from app.generated import robot_api_gateway_pb2 as pb
from app.generated import robot_api_gateway_pb2_grpc as pb_grpc

@pytest.mark.AsyncIterator
async def test_login_success(grpc_server, monkeypatch):
    
    async def mock_login_service(client, data):
        class MockResponse:
            def raise_for_status(self): pass
            def json(self):
                return {
                    'access_token':'test_access_token',
                    'refresh_token':'test_refresh_token'
                }
        return MockResponse()
    
    from app.services import conn_service
    monkeypatch.setattr(conn_service, 'login_service', mock_login_service)
    
    async with aio.insecure_channel(grpc_server) as channel:
        stub = pb_grpc.RobotApiGatewayStub(channel)
        request = pb.LoginRequest(
            robot_id='robot_test',
            robot_secret='secret'
        )
        
        response = await stub.Login(request)
        
        assert response.success == True
        assert response.access_token == 'test_access_token'
        assert response.refresh_token == 'test_refresh_token'            