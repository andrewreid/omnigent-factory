"""JSON codec for persisted aggregates, events and effect intents.

The store keeps the reducer's aggregate as canonical JSON (``schema_version`` tagged)
and projects selected fields into relational tables. This module is the single place
that maps frozen domain values to JSON-compatible data and back.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from functools import cache
from typing import Any, cast

from pydantic import TypeAdapter

from omnigent_factory.core.effects import EffectIntent, EffectKind, Preconditions, RetryClass
from omnigent_factory.core.events import BODY_TYPES, Event, EventBody, EventKind, Provenance
from omnigent_factory.core.types import AdmissionSnapshot, IssueSnapshot, Parcel

SCHEMA_VERSION = 1


@cache
def _adapter(tp: Any) -> TypeAdapter[Any]:
    return TypeAdapter(tp)


def _dump(tp: Any, value: object) -> Any:
    return _adapter(tp).dump_python(value, mode="json")


def _load(tp: Any, data: object) -> Any:
    return _adapter(tp).validate_python(data, strict=False)


def dumps(data: object) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# ---------------------------------------------------------------- aggregates


def parcel_to_json(parcel: Parcel) -> str:
    return dumps({"schema_version": SCHEMA_VERSION, "parcel": _dump(Parcel, parcel)})


def parcel_from_json(text: str) -> Parcel:
    data = json.loads(text)
    _check_version(data)
    return cast(Parcel, _load(Parcel, data["parcel"]))


def admission_to_data(admission: AdmissionSnapshot) -> Any:
    return _dump(AdmissionSnapshot, admission)


def admission_from_data(data: object) -> AdmissionSnapshot:
    return cast(AdmissionSnapshot, _load(AdmissionSnapshot, data))


# -------------------------------------------------------------------- events


def event_to_json(event: Event) -> str:
    body_type = BODY_TYPES[event.kind]
    return dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "event_id": event.event_id,
            "repo_id": event.repo_id,
            "parcel_id": event.parcel_id,
            "source_time_us": event.source_time_us,
            "provenance": event.provenance.value,
            "actor_id": event.actor_id,
            "issue_number": event.issue_number,
            "entropy": event.entropy,
            "delivery_guid": event.delivery_guid,
            "evidence": None if event.evidence is None else _dump(IssueSnapshot, event.evidence),
            "kind": event.kind.value,
            "body": _dump(body_type, event.body),
        }
    )


def event_from_json(text: str) -> Event:
    data = json.loads(text)
    _check_version(data)
    kind = EventKind(data["kind"])
    body = cast(EventBody, _load(BODY_TYPES[kind], data["body"]))
    evidence = data["evidence"]
    return Event(
        event_id=data["event_id"],
        repo_id=data["repo_id"],
        parcel_id=data["parcel_id"],
        source_time_us=data["source_time_us"],
        provenance=Provenance(data["provenance"]),
        body=body,
        actor_id=data["actor_id"],
        issue_number=data["issue_number"],
        entropy=data["entropy"],
        evidence=None if evidence is None else cast(IssueSnapshot, _load(IssueSnapshot, evidence)),
        delivery_guid=data["delivery_guid"],
    )


# ------------------------------------------------------------------- effects


def effect_to_json(effect: EffectIntent) -> str:
    return dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "effect_id": effect.effect_id,
            "kind": effect.kind.value,
            "parcel_id": effect.parcel_id,
            "target": effect.target,
            "preconditions": asdict(effect.preconditions),
            "args": dict(effect.args),
            "depends_on": list(effect.depends_on),
            "retry_class": effect.retry_class.value,
            "dedupe_key": effect.dedupe_key,
        }
    )


def effect_from_json(text: str) -> EffectIntent:
    data = json.loads(text)
    _check_version(data)
    return EffectIntent(
        effect_id=data["effect_id"],
        kind=EffectKind(data["kind"]),
        parcel_id=data["parcel_id"],
        target=data["target"],
        preconditions=Preconditions(**data["preconditions"]),
        args=data["args"],
        depends_on=tuple(data["depends_on"]),
        retry_class=RetryClass(data["retry_class"]),
        dedupe_key=data["dedupe_key"],
    )


def _check_version(data: Any) -> None:
    if not isinstance(data, dict) or data.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported payload schema_version")
