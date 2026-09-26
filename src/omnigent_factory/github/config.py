"""Strict parsing for the target repository's default-branch factory configuration."""

from __future__ import annotations

from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ConcurrencyConfig(_StrictModel):
    max_building: Annotated[int, Field(ge=1, le=100)]
    max_open_bot_prs: Annotated[int, Field(ge=1, le=100)]


class BlockHours(_StrictModel):
    S: Annotated[int, Field(ge=1, le=24)]
    M: Annotated[int, Field(ge=1, le=24)]
    L: Annotated[int, Field(ge=1, le=24)]

    @model_validator(mode="after")
    def ordered(self) -> BlockHours:
        if not self.S <= self.M <= self.L:
            raise ValueError("checkpoint blocks must be ordered S <= M <= L")
        return self


class CheckpointConfig(_StrictModel):
    block_hours: BlockHours
    grace_minutes: Annotated[int, Field(ge=1, le=120)]
    cost_backstop_usd_per_hour: Annotated[int, Field(gt=0, le=1000)]


class ReviewConfig(_StrictModel):
    bot_login: Annotated[str, Field(pattern=r"^[A-Za-z0-9-]+\[bot\]$")]
    approver_ids: Annotated[list[StrictInt], Field(min_length=1)]
    independent_reviewer_ids: list[StrictInt] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_numeric_owners(self) -> ReviewConfig:
        if any(owner <= 0 for owner in self.approver_ids):
            raise ValueError("approver ids must be positive numeric GitHub ids")
        if len(set(self.approver_ids)) != len(self.approver_ids):
            raise ValueError("approver ids must be unique")
        if any(reviewer <= 0 for reviewer in self.independent_reviewer_ids):
            raise ValueError("independent reviewer ids must be positive numeric GitHub ids")
        if len(set(self.independent_reviewer_ids)) != len(self.independent_reviewer_ids):
            raise ValueError("independent reviewer ids must be unique")
        if set(self.approver_ids) & set(self.independent_reviewer_ids):
            raise ValueError("owners cannot be configured as independent reviewers")
        return self


class GuidanceConfig(_StrictModel):
    triage: Annotated[str, Field(min_length=1)]
    engineering: Annotated[str, Field(min_length=1)]


class FactoryConfig(_StrictModel):
    version: Literal[1]
    concurrency: ConcurrencyConfig
    checkpoints: CheckpointConfig
    review: ReviewConfig
    guidance: GuidanceConfig


def parse_factory_config(raw: bytes) -> FactoryConfig:
    """Parse YAML with duplicate-key rejection and an exact, closed schema."""

    class UniqueKeyLoader(yaml.SafeLoader):
        pass

    def construct_mapping(
        loader: UniqueKeyLoader, node: yaml.nodes.MappingNode, deep: bool = False
    ) -> dict[object, object]:
        result: dict[object, object] = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            if key in result:
                raise ValueError(f"duplicate YAML key: {key}")
            result[key] = loader.construct_object(value_node, deep=deep)
        return result

    UniqueKeyLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping
    )
    data = yaml.load(raw, Loader=UniqueKeyLoader)  # noqa: S506 -- subclass of SafeLoader
    return FactoryConfig.model_validate(data)
