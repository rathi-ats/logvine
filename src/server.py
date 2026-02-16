"""Network server for logvine key-value store.

Exposes the Controller via TCP protocol with JSON-based request/response format.
Handles incoming connections, request parsing, and response transmission.
"""

import asyncio
import logging
import os   
from typing import Optional

from src.controller import Controller

logger = logging.getLogger(__name__)


class LogvineServer:
    """Async TCP server for logvine key-value store."""

    def __init__(
        self,
        controller: Controller,
        host: str = "127.0.0.1",
        port: int = 9000,
    ):
        """Initialize the server.

        Args:
            controller: The Controller instance to handle requests.
            host: Bind address.
            port: Bind port.
        """
        self.controller = controller
        self.host = host
        self.port = port
        self.server = None

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a single client connection.

        Reads requests line-by-line, processes them, and sends responses.

        Args:
            reader: StreamReader for the client connection.
            writer: StreamWriter for the client connection.
        """
        client_addr = writer.get_extra_info("peername")
        logger.info(f"Client connected: {client_addr}")

        try:
            while True:
                # Read request line (JSON terminated with newline)
                request_data = await reader.readline()
                if not request_data:
                    break  # Client closed connection

                try:
                    request_line = request_data.decode().strip()
                    logger.debug(f"Received request: {request_line}")

                    # Process request through controller
                    await self.controller.handle_request(request_line, writer)

                except Exception as e:
                    logger.error(f"Error processing request: {e}")
                    error_response = f'{{"error": "{str(e)}"}}\n'
                    writer.write(error_response.encode())
                    await writer.drain()

        except Exception as e:
            logger.error(f"Connection error: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Client disconnected: {client_addr}")

    async def start(self) -> None:
        """Start the server and listen for connections."""
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )

        addr = self.server.sockets[0].getsockname()
        logger.info(f"logvine server listening on {addr[0]}:{addr[1]}")

        async with self.server:
            await self.server.serve_forever()

    async def stop(self) -> None:
        """Stop the server."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("Server stopped")


async def run_server(
    storage_path: str,
    host: str = "127.0.0.1",
    port: int = 9000,
    node_id: Optional[int] = None,
    num_nodes: int = 1,
) -> None:
    """Run a logvine server with the given configuration.

    Args:
        storage_path: Path to the storage directory.
        host: Bind address.
        port: Bind port.
        node_id: Node ID for distributed setup (None for single-node).
        num_nodes: Total nodes in the cluster.
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create storage directory if it doesn't exist
    os.makedirs(storage_path, exist_ok=True)

    controller = Controller(
        storage_path, node_id=node_id, num_nodes=num_nodes
    )
    server = LogvineServer(controller, host=host, port=port)

    try:
        await server.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        await server.stop()


if __name__ == "__main__":
    import sys

    storage_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/logvine"
    asyncio.run(run_server(storage_path))
