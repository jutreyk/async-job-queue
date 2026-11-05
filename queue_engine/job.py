import json
import time
import uuid
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Dict, Any, Optional

class TaskStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"

@dataclass
class Job:
    id: str
    task_name: str
    payload: Dict[str, Any]
    status: TaskStatus
    retry_count: int = 0
    max_retries: int = 3
    created_at: float = 0.0
    error: Optional[str] = None

    @classmethod
    def create(cls, task_name: str, payload: Dict[str, Any], max_retries: int = 3):
        return cls(
            id=str(uuid.uuid4())[:8],
            task_name=task_name,
            payload=payload,
            status=TaskStatus.QUEUED,
            retry_count=0,
            max_retries=max_retries,
            created_at=time.time()
        )

    def to_json(self) -> str:
        d = asdict(self)
        d['status'] = self.status.value
        return json.dumps(d)

    @classmethod
    def from_json(cls, data_str: str):
        d = json.loads(data_str)
        d['status'] = TaskStatus(d['status'])
        return cls(**d)
