import asyncio
import logging
from typing import Dict, Callable, Any
from .job import Job, TaskStatus

logger = logging.getLogger("AsyncQueue")

class AsyncBroker:
    """In-memory async message broker with Dead Letter Queue (DLQ) and exponential backoff."""
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
            job.error = f"No handler registered for '{job.task_name}'"
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
                # Exponential backoff simulation
                backoff_sec = 0.05 * (2 ** (job.retry_count - 1))
                await asyncio.sleep(backoff_sec)
                await self.queue.put(job)
            else:
                # Exceeded retries -> send to Dead Letter Queue
                job.status = TaskStatus.DEAD_LETTER
                self.dead_letter_queue.append(job)

        finally:
            self.queue.task_done()

        return job

class Worker:
    """Async worker pool consumer processing jobs concurrently."""
    def __init__(self, broker: AsyncBroker, concurrency: int = 2):
        self.broker = broker
        self.concurrency = concurrency
        self._running = False
        self._tasks = []

    async def start(self):
        self._running = True
        for _ in range(self.concurrency):
            t = asyncio.create_task(self._worker_loop())
            self._tasks.append(t)

    async def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _worker_loop(self):
        while self._running:
            try:
                await self.broker.process_one()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker exception: {e}")
