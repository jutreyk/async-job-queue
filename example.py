import asyncio
from rich.console import Console
from rich.table import Table
from queue_engine.broker import AsyncBroker, Worker

console = Console()

async def main():
    broker = AsyncBroker()

    async def handle_invoice(p):
        await asyncio.sleep(0.05)

    async def handle_email(p):
        await asyncio.sleep(0.02)

    broker.register("generate_invoice", handle_invoice)
    broker.register("send_email", handle_email)

    console.print("[bold cyan]Starting Async Job Queue Worker Pool[/bold cyan]\n")

    # Enqueue sample jobs
    for i in range(1, 6):
        await broker.enqueue("generate_invoice", {"invoice_id": 1000 + i})
        await broker.enqueue("send_email", {"to": f"user{i}@example.com"})

    # Process all jobs
    table = Table(title="Processed Job Log", border_style="dim")
    table.add_column("Job ID", style="cyan")
    table.add_column("Task", style="magenta")
    table.add_column("Status", style="green")

    while not broker.queue.empty():
        job = await broker.process_one()
        table.add_row(job.id, job.task_name, job.status.value)

    console.print(table)
    console.print("\n[bold green]All background jobs completed successfully![/bold green]")

if __name__ == "__main__":
    asyncio.run(main())
