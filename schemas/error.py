from pydantic import BaseModel, Field

class ErrorOutput(BaseModel):
    message:str = Field(..., description="Detailed Error Message")