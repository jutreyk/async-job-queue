# async-job-queue

Lightweight distributed asynchronous job queue and worker pool written in Python 3.10+ using `asyncio`. Engineered as a transparent alternative to Celery/RQ, featuring exponential backoff retries, Dead Letter Queue (DLQ) containment, and concurrent worker task processing.

## Highlights

- **Pure Async Concurrency**: Built entirely upon `asyncio.Queue` and non-blocking worker pools.
- **Fault Tolerance**: Automatic retry scheduling with exponential backoff ($T = \text{base} \times 2^{\text{retry}}$).
- **Dead Letter Queue (DLQ)**: Failed tasks exceeding retry thresholds are cleanly isolated into the DLQ for operator inspection without stalling workers.
- **Structured Models**: Fully serializable `Job` instances tracking execution timings, worker status, and failure traces.

## Quickstart

```bash
# Run tests
python -m pytest

# Run sample worker pool pipeline
python example.py
```

## License

MIT © [Arkadiy Gazizov](https://arkadiygazizov.ru)
