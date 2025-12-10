from pydantic import BaseModel, Field

class HeartbeatInput(BaseModel):
    robot_id:str = Field(..., description="Robot ID")
    
class HeartbeatOutput(BaseModel):
    result: str = Field(..., description="Searching result")