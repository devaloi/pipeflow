"""Pipeline orchestrator — chains extract → transform → validate → load."""

from __future__ import annotations


from pipeflow.config import PipelineConfig
from pipeflow.observability.logger import get_logger
from pipeflow.observability.metrics import PipelineMetrics
from pipeflow.types import Record
logger = get_logger(__name__, json_format=False)


class Pipeline:
    """Orchestrates an ETL pipeline from config."""

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.metrics = PipelineMetrics()

    def run(self) -> dict[str, object]:
        """Execute the full pipeline. Returns metrics dict."""
        from pipeflow.extractors import build_extractor
        from pipeflow.transforms import build_transforms
        from pipeflow.loaders import build_loader
        from pipeflow.validation.validator import build_validator

        self.metrics.start()
        logger.info("Pipeline '%s' starting", self.config.name)

        # Extract
        extractor = build_extractor(self.config.extract)
        transforms = build_transforms(self.config.transforms)
        validator = build_validator(self.config.validate) if self.config.validate else None
        loader = build_loader(self.config.load)

        valid_records: list[Record] = []

        try:
            for record in extractor.extract():
                self.metrics.records_extracted += 1

                # Transform chain
                current: Record | None = record
                for transform in transforms:
                    if current is None:
                        break
                    current = transform.apply(current)

                if current is None:
                    continue
                self.metrics.records_transformed += 1

                # Validate
                if validator:
                    errors = validator.validate_record(current)
                    if errors:
                        self.metrics.records_invalid += 1
                        self.metrics.errors.append(
                            {"record": current, "errors": errors}
                        )
                        continue
                self.metrics.records_valid += 1
                valid_records.append(current)

                # Batch load
                if len(valid_records) >= self.config.load.batch_size:
                    loaded = loader.load(valid_records)
                    self.metrics.records_loaded += loaded
                    valid_records.clear()

            # Load remaining
            if valid_records:
                loaded = loader.load(valid_records)
                self.metrics.records_loaded += loaded

        finally:
            loader.close()
            self.metrics.stop()

        logger.info(
            "Pipeline '%s' complete: %s",
            self.config.name,
            self.metrics.to_dict(),
        )
        return self.metrics.to_dict()
