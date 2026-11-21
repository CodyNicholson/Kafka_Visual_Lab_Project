import React, { useEffect, useState, useRef } from 'react';

const API_BASE = '/api/kafka';

const LESSONS = {
  ROUND_ROBIN: {
    title: '1. Keyless partitioning and load distribution',
    concept: 'When a Producer sends a message with key = null, Kafka distributes records across available partitions (P0, P1, P2). Each partition is an ordered, immutable log file on the broker disk. Distributing across partitions allows parallel consumption by multiple threads in a Consumer Group.'
  },
  KEYED_ORDER: {
    title: '2. Key hashing and strict ordering',
    concept: 'Kafka only guarantees strict ordering WITHIN a single partition, not across an entire topic. By supplying a message key (e.g., USER-101), the Producer computes murmur2(key) % numPartitions. Every event for USER-101 is guaranteed to land in the exact same partition and be processed sequentially by the same Consumer thread.'
  },
  POISON_PILL: {
    title: '3. Non-blocking retries and the dead-letter topic (DLT)',
    concept: 'When a @KafkaListener throws an exception on a corrupt message ("POISON_PILL"), blocking the partition would stall all subsequent messages. Spring Kafka @RetryableTopic forwards the failing record to a retry topic with backoff, and once retries are exhausted, quarantines it into demo-orders-dlt where @DltHandler inspects it.'
  },
  OFFSET_REPLAY: {
    title: '4. The append-only log and offset replay',
    concept: 'Unlike traditional message queues (RabbitMQ/SQS) that delete messages once acknowledged, Kafka retains messages on disk based on retention policies. Consumers simply track a numeric cursor called an Offset. Clicking Replay uses ConsumerSeekAware to rewind the cursor to Offset 0 and re-process the entire history.'
  }
};

export default function App() {
  const [selectedLesson, setSelectedLesson] = useState('ROUND_ROBIN');
  const [activeStage, setActiveStage] = useState(null);
  const [activePartition, setActivePartition] = useState(null);
  const [activeWorker, setActiveWorker] = useState(null);
  const [activePacket, setActivePacket] = useState(null);
  const [partitions, setPartitions] = useState({ 0: [], 1: [], 2: [] });
  const [dltMessages, setDltMessages] = useState([]);
  const [logs, setLogs] = useState([]);
  const counterRef = useRef(1);

  useEffect(() => {
    const eventSource = new EventSource(`${API_BASE}/stream`);

    eventSource.addEventListener('kafka-trace', (e) => {
      const event = JSON.parse(e.data);
      setLogs((prev) => [event, ...prev.slice(0, 39)]);
      setActiveStage(event.stage);
      setActivePacket({ key: event.key || 'NULL', payload: event.payload, stage: event.stage });

      if (event.partition !== null && event.partition !== undefined) {
        setActivePartition(event.partition);
      }
      if (event.consumerThread) {
        setActiveWorker(event.consumerThread);
      }

      if (event.stage === 'BROKER_ACK' && event.partition !== null) {
        setPartitions((prev) => ({
          ...prev,
          [event.partition]: [
            ...prev[event.partition].slice(-5),
            { offset: event.offset, key: event.key || 'null', payload: event.payload }
          ]
        }));
      }

      if (event.stage === 'DLT') {
        setDltMessages((prev) => [
          ...prev.slice(-4),
          { offset: event.offset, partition: event.partition, payload: event.payload }
        ]);
      }
    });

    return () => eventSource.close();
  }, []);

  const triggerSend = async (key, label, roundRobin, lessonKey) => {
    setSelectedLesson(lessonKey);
    const num = counterRef.current++;
    const payload = `${label} #${num}`;
    await fetch(
      `${API_BASE}/send?key=${encodeURIComponent(key || '')}&payload=${encodeURIComponent(payload)}&roundRobin=${roundRobin}`,
      { method: 'POST' }
    );
  };

  const triggerReplay = async () => {
    setSelectedLesson('OFFSET_REPLAY');
    await fetch(`${API_BASE}/replay`, { method: 'POST' });
  };

  const lesson = LESSONS[selectedLesson];

  return (
    <div className="lab-container">
      <header className="lab-header">
        <div>
          <h1>Apache Kafka message flow</h1>
          <p>Spring Boot 3 and Spring Kafka against a real KRaft broker, traced live over SSE</p>
        </div>
        <div className="status-pill">KRaft broker, 3 partitions</div>
      </header>

      {/* Scenario Control Bar */}
      <section className="controls-grid">
        <button
          className="btn btn-blue"
          onClick={() => triggerSend('', 'Order-Event', true, 'ROUND_ROBIN')}
        >
          1. Send a keyless message (round-robin P0 → P1 → P2)
        </button>

        <button
          className="btn btn-purple"
          onClick={() => triggerSend('USER-101', 'User101-Checkout', false, 'KEYED_ORDER')}
        >
          2A. Send a keyed message (key USER-101)
        </button>

        <button
          className="btn btn-indigo"
          onClick={() => triggerSend('USER-202', 'User202-Payment', false, 'KEYED_ORDER')}
        >
          2B. Send a keyed message (key USER-202)
        </button>

        <button
          className="btn btn-danger"
          onClick={() => triggerSend('BAD-TX', 'POISON_PILL_FAIL', false, 'POISON_PILL')}
        >
          3. Send a poison pill (retry → dead-letter topic)
        </button>

        <button className="btn btn-amber" onClick={triggerReplay}>
          4. Replay every offset from 0 (seek to beginning)
        </button>
      </section>

      {/* Dynamic Educational Banner */}
      <section className="lesson-banner">
        <h3>{lesson.title}</h3>
        <p>{lesson.concept}</p>
        {activePacket && (
          <div className="live-packet-indicator" aria-live="polite">
            <span className="pulse-dot" />
            <strong>In flight:</strong> key <code>{activePacket.key}</code> · payload{' '}
            <code>{activePacket.payload}</code> · stage <span className="stage-tag">{activeStage}</span>
          </div>
        )}
      </section>

      {/* Interactive Animated Topology Canvas */}
      <section className="topology-board">
        {/* Column 1: Producer */}
        <div className={`node-card producer-card ${activeStage === 'PRODUCER_SENT' ? 'glow-active' : ''}`}>
          <div className="node-title">Spring Kafka producer</div>
          <p className="node-sub">KafkaTemplate&lt;String, String&gt;</p>
          <div className="mechanic-box">
            <div>Serializer: <code>StringSerializer</code></div>
            <div>Acks: <code>all (Idempotent)</code></div>
            <div>Partitioner: <code>Murmur2 / Sticky</code></div>
          </div>
          {activeStage === 'PRODUCER_SENT' && (
            <div className="packet-badge animate-fly">Sent: {activePacket?.payload}</div>
          )}
        </div>

        <div className={`flow-arrow ${activeStage === 'PRODUCER_SENT' || activeStage === 'BROKER_ACK' ? 'arrow-hot' : ''}`}>
          <span>ProduceRequest</span>
          <div className="arrow-line" />
        </div>

        {/* Column 2: Kafka Broker & Topic Partitions */}
        <div className={`node-card broker-card ${activeStage === 'BROKER_ACK' ? 'glow-active' : ''}`}>
          <div className="node-title">Kafka broker, topic demo-orders</div>
          <p className="node-sub">Append-only partition logs</p>

          {[0, 1, 2].map((pId) => (
            <div
              key={pId}
              className={`partition-strip ${activePartition === pId && activeStage === 'BROKER_ACK' ? 'partition-highlight' : ''}`}
            >
              <div className="partition-label">Partition {pId}</div>
              <div className="offset-track">
                {partitions[pId].length === 0 ? (
                  <span className="empty-log">Empty log</span>
                ) : (
                  partitions[pId].map((item, idx) => (
                    <div key={idx} className="offset-cell" title={`${item.key}: ${item.payload}`}>
                      <span className="offset-num">#{item.offset}</span>
                      <span className="offset-key">{item.key}</span>
                    </div>
                  ))
                )}
              </div>
            </div>
          ))}

          {/* Dead Letter Topic Strip */}
          <div className={`partition-strip dlt-strip ${activeStage === 'DLT' ? 'dlt-highlight' : ''}`}>
            <div className="partition-label">Topic demo-orders-dlt</div>
            <div className="offset-track">
              {dltMessages.length === 0 ? (
                <span className="empty-log">No poison pills quarantined yet</span>
              ) : (
                dltMessages.map((m, idx) => (
                  <div key={idx} className="offset-cell dlt-cell">
                    <span>P{m.partition}:#{m.offset}</span>
                    <span>{m.payload}</span>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>

        <div className={`flow-arrow ${['CONSUMER_A', 'CONSUMER_B', 'RETRY', 'DLT'].includes(activeStage) ? 'arrow-hot' : ''}`}>
          <span>FetchRequest (poll)</span>
          <div className="arrow-line" />
        </div>

        {/* Column 3: Consumer Groups */}
        <div className="consumers-column">
          {/* Consumer Group A */}
          <div className={`node-card consumer-group-card ${activeStage === 'CONSUMER_A' || activeStage === 'RETRY' ? 'glow-active' : ''}`}>
            <div className="node-title">Group A: fulfillment-group</div>
            <p className="node-sub">Competing consumers (concurrency = 3)</p>
            {[0, 1, 2].map((wId) => {
              const workerId = `Fulfillment-Worker-${wId}`;
              const isSelected = activeWorker === workerId && (activeStage === 'CONSUMER_A' || activeStage === 'RETRY');
              return (
                <div
                  key={wId}
                  className={`worker-box ${isSelected ? (activeStage === 'RETRY' ? 'worker-error' : 'worker-active') : ''}`}
                >
                  <span>@KafkaListener thread #{wId}</span>
                  <span className="assigned-badge">P{wId}</span>
                </div>
              );
            })}
          </div>

          {/* Consumer Group B */}
          <div className={`node-card consumer-group-card ${activeStage === 'CONSUMER_B' ? 'glow-active' : ''}`}>
            <div className="node-title">Group B: analytics-group</div>
            <p className="node-sub">Independent listener: gets every record too</p>
            <div className={`worker-box ${activeStage === 'CONSUMER_B' ? 'worker-active' : ''}`}>
              <span>Analytics consumer</span>
              <span className="assigned-badge">P0, P1, P2</span>
            </div>
          </div>

          {/* DLT Handler */}
          <div className={`node-card dlt-card ${activeStage === 'DLT' ? 'glow-danger' : ''}`}>
            <div className="node-title">@DltHandler quarantine</div>
            <p className="node-sub">Receives records whose @RetryableTopic retries ran out</p>
          </div>
        </div>
      </section>

      {/* Real-time Telemetry Feed */}
      <section className="telemetry-panel">
        <h3>Live broker and listener telemetry (SSE)</h3>
        <div className="log-list">
          {logs.length === 0 ? (
            <div className="log-empty">Send a message above to watch its journey through the broker.</div>
          ) : (
            logs.map((log, i) => (
              <div key={i} className={`log-row stage-${log.stage}`}>
                <span className="log-badge">{log.stage}</span>
                <span className="log-meta">
                  {log.partition !== null ? `[P${log.partition} | Offset #${log.offset}]` : '[Producer]'}
                </span>
                <span className="log-desc">{log.description}</span>
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
}
