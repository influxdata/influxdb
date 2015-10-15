# Configuration

Each UDP input allows the binding address, target database, and target retention policy to be set. If the database does not exist, it will be created automatically when the input is initialized. If the retention policy is not configured, then the default retention policy for the database is used. However if the retention policy is set, the retention policy must be explicitly created. The input will not automatically create it.

Each UDP input also performs internal batching of the points it receives, as batched writes to the database are more efficient. The default _batch size_ is 1000, _pending batch_ factor is 5, with a _batch timeout_ of 1 second. This means the input will write batches of maximum size 1000, but if a batch has not reached 1000 points within 1 second of the first point being added to a batch, it will emit that batch regardless of size. The pending batch factor controls how many batches can be in memory at once, allowing the input to transmit a batch, while still building other batches.

# Processing

The UDP input can receive up to 64KB per read, and splits the received data by newline. Each part is then interpreted as line-protocol encoded points, and parsed accordingly.

# UDP is connectionless

Since UDP is a connectionless protocol there is no way to signal to the data source if any error occurs, and if data has even been successfully indexed. This should be kept in mind when deciding if and when to use the UDP input. The built-in UDP statistics are useful for monitoring the UDP inputs.
