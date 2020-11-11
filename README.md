# Message storage

This box is responsible for saving parsed and raw message to the Cradle.

## Configuration

```json
{
  "drain-interval": 1000,
  "termination-timeout": 5000
}
```

#### drain-interval
Interval in milliseconds to drain all aggregated batches that are not stored yet. The default value is 1000.

#### termination-timeout
The timeout in milliseconds to await the inner drain scheduler has finished all tasks. The default value is 5000.