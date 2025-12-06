from pydantic import BaseModel, Field
from typing import List, Optional
from schemas.robot import Robot

class RobotCheckInput(BaseModel):
    robot_id:str = Field(..., description="Robot ID")
    robot_secret:str = Field(..., description="Robot Password")
    
class RobotCheckOutput(BaseModel):
    robots: Optional[List[Robot]] = Field(None, description="List of Robots")
    current_totalCount: Optional[int] = Field(None, description="Total Number of robots that matched to the condition")
    totalCount: Optional[int] = Field(None, description="Total Number of Robots")
    result: str = Field(..., description="Searching result")