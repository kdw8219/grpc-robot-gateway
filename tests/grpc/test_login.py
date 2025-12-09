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
        
        
@pytest.mark.AsyncIterator
async def test_heartbeat_success(grpc_server, monkeypatch):
    
    async def mock_heartbeat_service(*args, **kwargs):
        class MockResponse:
            def raise_for_status(self): pass
            def json(self):
                return {
                    'success' : True,
                    'result':'heartbeat success',
                }
        return MockResponse()
    
    # from app.services import conn_service
    # monkeypatch.setattr(conn_service, 'heartbeat_service', mock_heartbeat_service)
    
    import app.server.main as main_module
    monkeypatch.setattr(main_module.conn_service, 'heartbeat_service', mock_heartbeat_service)
    
    async with aio.insecure_channel(grpc_server) as channel:
        stub = pb_grpc.RobotApiGatewayStub(channel)
        request = pb.HeartbeatRequest(
            robot_id='robot_test',
            internal_ip='stream_ip_test'
        )
        
        response = await stub.Heartbeat(request)
        
        assert response.success is True
        assert response.result == 'heartbeat success'