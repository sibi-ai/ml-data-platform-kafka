"""Schema validation for incoming Kafka events."""
from pydantic import BaseModel, ValidationError
import structlog

log = structlog.get_logger()


class EventSchema(BaseModel):
    entity_id: str
    ts: float
    feature_a: float
    feature_b: float
    feature_c: float


class SchemaValidator:
    def validate(self, record: dict) -> bool:
        try:
            EventSchema(**record)
            return True
        except ValidationError as e:
            log.warning("validation_error", errors=e.errors())
            return False
