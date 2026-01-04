# Message Listener Async Flow

This note documents the current asynchronous pipeline that drains the RabbitMQ queue so it is easier to debug or extend without reverse‑engineering the code.

## 1. Startup
1. `MessagingServiceCollectionExtensions.AddBrokerMessaging` registers `MessageListener` as an `IMessageListener`.
2. `QueueWorker` calls `ReadAsync` the first time it needs deliveries, which triggers `MessageListener` to spin up the consumer loop via `StartListenerLoopIfNeeded`.

## 2. Listener Loop
1. The loop runs on a background `Task` with its own `CancellationTokenSource`.
2. Each iteration asks `ListenerChannelManager` for a channel; that helper owns the single `_channelLock`, creates connections/channels when needed, and ensures the exchange/queue/binding exist via `QueueBindingsManager`.
3. The channel registers an `AsyncEventingBasicConsumer` with `BasicConsumeAsync` using the configured queue name and QoS/prefetch = `DefaultConcurrency`.
4. The loop waits until either the consumer signals shutdown or cancellation is requested. On shutdown/fault, it logs the issue, calls `ListenerChannelManager.ResetAsync`, and retries after a small delay.

## 3. Delivery Buffering
1. Each AMQP delivery triggers `OnMessageReceivedAsync`, which wraps the payload in `MessageDelivery`.
2. The delivery is written to an internal bounded `Channel<IInboundDelivery>` whose capacity is `prefetch * 4`. When the buffer is full, `WriteAsync` naturally back‑pressures the RabbitMQ client.
3. `ReadAsync` simply drains that channel with `await foreach`, so `QueueWorker` receives deliveries as an `IAsyncEnumerable`.

## 4. Shutdown
1. `DisposeAsync` cancels the listener loop CTS, waits for the task to complete, and then calls `ListenerChannelManager.DisposeAsync` to close the channel/connection.
2. The delivery channel is completed so `QueueWorker` eventually finishes its loop gracefully.

## 5. What To Watch
- Connection churn: any repeated log of “Message consumer faulted; restarting” indicates broker/network issues.
- Buffer pressure: if the worker cannot keep up, `WriteAsync` will block and you will see RabbitMQ queue depth rise.
- Prefetch vs concurrency: `DefaultConcurrency` controls both the worker pool size and `BasicQos` prefetch; keep them aligned.

This flow is already fully asynchronous: all I/O is awaited, and the only locking is the small `_channelLock` inside `ListenerChannelManager` that protects channel creation/disposal. Future refactors can split out buffering or add observability hooks if needed.
