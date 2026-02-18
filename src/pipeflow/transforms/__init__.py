"""Transforms package."""

from __future__ import annotations

from pipeflow.config import TransformConfig
from pipeflow.transforms.base import Transform


def build_transforms(configs: list[TransformConfig]) -> list[Transform]:
    """Factory: build a list of transforms from config."""
    from pipeflow.transforms.rename import RenameTransform
    from pipeflow.transforms.cast import CastTransform
    from pipeflow.transforms.filter import FilterTransform
    from pipeflow.transforms.derive import DeriveTransform
    from pipeflow.transforms.deduplicate import DeduplicateTransform

    transforms = []
    for cfg in configs:
        match cfg.type:
            case "rename":
                transforms.append(RenameTransform(mapping=cfg.mapping or {}))
            case "cast":
                transforms.append(CastTransform(columns=cfg.columns or {}))
            case "filter":
                transforms.append(FilterTransform(condition=cfg.condition or "True"))
            case "derive":
                transforms.append(DeriveTransform(expression=cfg.expression or ""))
            case "deduplicate":
                key = cfg.key or []
                if isinstance(key, str):
                    key = [key]
                transforms.append(DeduplicateTransform(key=key))
            case _:
                raise ValueError(f"Unknown transform type: {cfg.type}")
    return transforms