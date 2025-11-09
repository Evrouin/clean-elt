def calculate_optimal_batch_size(file_size_bytes: int) -> int:
    """Calculate optimal batch size based on file size"""
    if file_size_bytes < 1024 * 1024:  # < 1MB
        return 50
    elif file_size_bytes < 10 * 1024 * 1024:  # < 10MB
        return 100
    elif file_size_bytes < 100 * 1024 * 1024:  # < 100MB
        return 200
    else:  # > 100MB
        return 500
