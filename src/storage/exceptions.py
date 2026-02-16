
class BatchTooLargeException(Exception):
    def __init__(self, batch_bytes: int, max_bytes: int):
        self.batch_bytes = batch_bytes
        self.max_bytes = max_bytes
        message = (
            f"Input batch ({batch_bytes} bytes) exceeds maximum allowed "
            f"({max_bytes} bytes). Consider splitting into multiple requests."
        )
        super().__init__(message)

