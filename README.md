# High Level Architecture
![high-level-architecture](./images/high-level.drawio.svg)

Client
- Maintain only one websocket connection to the producer.
- Every message regradless room is sent through the connection to the producer
- Maintain multiple connections to the consumer. Each connection represention a room.

Producer
- Receive messages from the client and publish them to the message queue.
- Use circuit breaker for message publication. Data safety guaranteed.
- Queue connection pooling.

Consumer
- Manage client connections and forward messages from the message queue with retry.
- Acknowledgement after message being sent.
- Maintain an idempotency cache for one-time delivery guarantee.

# Sequence Diagram
![sequence-diagram](./images/sequence-digram.drawio.svg)