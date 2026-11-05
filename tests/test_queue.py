import pytest
import asyncio
from queue_engine.broker import AsyncBroker, Worker
from queue_engine.job import TaskStatus

@pytest.mark.asyncio
async def test_successful_task_execution():
    broker = AsyncBroker()
    results = []

    broker.register("send_notification", lambda payload: results.append(payload["user"]))
    
    await broker.enqueue("send_notification", {"user": "Arkadiy"})
    job = await broker.process_one()

    assert job.status == TaskStatus.SUCCESS
    assert results == ["Arkadiy"]

@pytest.mark.asyncio
async def test_retry_and_dead_letter_queue():
    broker = AsyncBroker()
    attempts = 0

    def failing_handler(payload):
        nonlocal attempts
        attempts += 1
        raise ConnectionResetError("Database connection lost")

    broker.register("sync_db", failing_handler)
    
    # Max retries = 2
    job = await broker.enqueue("sync_db", {"table": "users"}, max_retries=2)
    
    # 1st attempt (fails, re-enqueued)
    j1 = await broker.process_one()
    assert j1.retry_count == 1
    
    # 2nd attempt (fails, re-enqueued)
    j2 = await broker.process_one()
    assert j2.retry_count == 2
    
    # 3rd attempt (exceeds max_retries 2 -> dead letter queue)
    j3 = await broker.process_one()
    assert j3.status == TaskStatus.DEAD_LETTER
    assert len(broker.dead_letter_queue) == 1
    assert broker.dead_letter_queue[0].id == job.id
