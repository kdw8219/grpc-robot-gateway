from pydantic import BaseModel, Field

class HeartbeatInput(BaseModel):
    robot_id:str = Field(..., description="Robot ID")
    stream_ip:str = Field(...,description="Stream IP")
    
class HeartbeatOutput(BaseModel):
    result: str = Field(..., description="Searching result")