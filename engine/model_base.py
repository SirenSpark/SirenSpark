from pydantic import BaseModel
from typing import Dict, Any, Optional


class BaseStep(BaseModel):
    id: str
    type: str
    input: Optional[Dict[str, Any]]
    output: Optional[Dict[str, Any]]
