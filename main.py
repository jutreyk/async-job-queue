import asyncio
import json
import time
import uuid
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Dict, Any, Optional, Callable
from rich.console import Console
from rich.table import Table

console = Console()

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

class AsyncQueue:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.dead_letter_queue = []
        self.completed_jobs = {}
        self.handlers: Dict[str, Callable[[Dict[str, Any]], Any]] = {}

    def register(self, task_name: str, handler: Callable[[Dict[str, Any]], Any]):
        self.handlers[task_name] = handler

    async def enqueue(self, task_name: str, payload: Dict[str, Any], max_retries: int = 3) -> Job:
        job = Job.create(task_name, payload, max_retries)
        await self.queue.put(job)
        return job

    async def process_one(self) -> Job:
        job: Job = await self.queue.get()
        job.status = TaskStatus.PROCESSING
        handler = self.handlers.get(job.task_name)

        if not handler:
            job.status = TaskStatus.FAILED
            job.error = f"Обработчик для '{job.task_name}' не найден"
            self.dead_letter_queue.append(job)
            self.queue.task_done()
            return job

        try:
            if asyncio.iscoroutinefunction(handler):
                await handler(job.payload)
            else:
                handler(job.payload)
            job.status = TaskStatus.SUCCESS
            self.completed_jobs[job.id] = job
        except Exception as e:
            job.retry_count += 1
            job.error = str(e)
            if job.retry_count <= job.max_retries:
                job.status = TaskStatus.QUEUED
                backoff_sec = 0.05 * (2 ** (job.retry_count - 1))
                await asyncio.sleep(backoff_sec)
                await self.queue.put(job)
            else:
                job.status = TaskStatus.DEAD_LETTER
                self.dead_letter_queue.append(job)
        finally:
            self.queue.task_done()

        return job

async def demo():
    q = AsyncQueue()

    async def handle_invoice(p):
        await asyncio.sleep(0.04)

    async def handle_email(p):
        await asyncio.sleep(0.02)

    q.register("generate_invoice", handle_invoice)
    q.register("send_email", handle_email)

    console.print("[bold cyan]Запуск очереди фоновых задач[/bold cyan]\n")
    for i in range(1, 6):
        await q.enqueue("generate_invoice", {"invoice_id": 1000 + i})
        await q.enqueue("send_email", {"to": f"user{i}@example.com"})

    table = Table(title="Журнал обработки задач", border_style="dim")
    table.add_column("ID задачи", style="cyan")
    table.add_column("Тип", style="magenta")
    table.add_column("Статус", style="green")

    while not q.queue.empty():
        job = await q.process_one()
        table.add_row(job.id, job.task_name, job.status.value)

    console.print(table)
    console.print(f"\n[green]Завершено. Задач в Dead Letter Queue: {len(q.dead_letter_queue)}[/green]")

if __name__ == "__main__":
    asyncio.run(demo())
