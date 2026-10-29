"""Kafka consumer — ingests events and writes to feature store."""
import json
from kafka import KafkaConsumer
from src.feature_store.writer import FeatureWriter
from src.quality.validator import SchemaValidator
import structlog

log = structlog.get_logger()


class EventConsumer:
    """Kafka consumer that validates incoming events and writes features."""

    def __init__(self, topic: str, group_id: str, brokers: list[str],
                 writer: FeatureWriter, validator: SchemaValidator):
        self.consumer = KafkaConsumer(
            topic,
            group_id=group_id,
            bootstrap_servers=brokers,
            value_deserializer=lambda m: json.loads(m.decode()),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=500,
        )
        self.writer    = writer
        self.validator = validator
        self._running  = False

    def run(self):
        self._running = True
        log.info("consumer_started")
        try:
            while self._running:
                batch = self.consumer.poll(timeout_ms=1000)
                for tp, messages in batch.items():
                    valid, invalid = [], []
                    for msg in messages:
                        if self.validator.validate(msg.value):
                            valid.append(msg.value)
                        else:
                            invalid.append(msg.value)
                            log.warning("invalid_event", offset=msg.offset)
                    if valid:
                        self.writer.write_batch(valid)
                    self.consumer.commit()
                    log.info("batch_processed",
                             partition=tp.partition, valid=len(valid), invalid=len(invalid))
        finally:
            self.consumer.close()
            log.info("consumer_stopped")

    def stop(self):
        self._running = False
