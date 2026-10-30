"""Ranking capability lifecycle, durable template pins, and deployment diagnostics."""

import hashlib
import json
import subprocess
from dataclasses import replace

import pytest

from omnigent_factory.core.effects import CredentialProfile
from omnigent_factory.credentials.broker import LocalCredentialBroker
from omnigent_factory.credentials.capabilities import CapabilityRegistry, read_capability_file
from omnigent_factory.omnigent.ranking import RankingSessionError
from omnigent_factory.ports.credentials import TokenGrant, TokenRefusal
from omnigent_factory.service.credentials import StoreExecutionGate
from omnigent_factory.service.doctor import DoctorReport, _check_github_credentials
from omnigent_factory.service.durable import StoreCapabilityStore
from omnigent_factory.service.ranking import RUN_TIMEOUT_US
from omnigent_factory.store import ranking as rs
from tests.credentials.fakes import FakeMinter
from tests.service.test_ranking import card, item, ranking_config, rig, started


def broker_for(r, tmp_path):
    config = r.service.config
    return LocalCredentialBroker(
        gate=StoreExecutionGate(r.service.db, r.clock),
        minter=FakeMinter(r.clock),
        clock=r.clock,
        repository=config.repository,
        capabilities=CapabilityRegistry(
            config.capability_dir,
            tmp_path / "broker.sock",
            config.repository,
            store=StoreCapabilityStore(r.service.db),
        ),
    )


async def token(broker, run):
    record = broker.capabilities.get(run.run_id)
    assert record is not None
    cap = read_capability_file(record.path)
    return await broker.request_token(run.run_id, cap.secret, cap.repository)


@pytest.mark.asyncio
async def test_ranking_scoped_token_restart_submission_and_retirement(service_config, tmp_path):
    async with rig(ranking_config(service_config)) as r:
        broker = broker_for(r, tmp_path)
        r.ranker.broker = broker
        run = await started(r, card(1))
        grant = await token(broker, run)
        assert isinstance(grant, TokenGrant) and grant.profile == CredentialProfile.READ_ONLY
        text = await r.ranker._start_message(run)
        assert str(service_config.wrapper_bin_dir / "gh") in text
        assert str(service_config.capability_dir / f"{run.run_id}.cap") in text
        # Durable restore starts closed, then requires exact policies and the DB gate.
        await broker.restore()
        assert isinstance(await token(broker, run), TokenRefusal)
        r.ranker = r.new_ranker()
        r.ranker.broker = broker
        assert await r.ranker._advance(run) == "running"
        assert isinstance(await token(broker, run), TokenGrant)
        await r.submit({"ranking": [item(1)], "summary": "One item"})
        # Even a cached token cannot be issued once the submission closes the work gate.
        assert isinstance(await token(broker, run), TokenRefusal)
        await r.ranker.run_once()
        assert broker.capabilities.get(run.run_id) is None
        assert not broker.issuance_enabled(run.run_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["expired", "policy"])
async def test_ranking_boot_fails_closed(service_config, tmp_path, monkeypatch, failure):
    async with rig(ranking_config(service_config)) as r:
        broker = broker_for(r, tmp_path)
        r.ranker.broker = broker
        run = await started(r, card(1))
        await broker.restore()
        r.ranker = r.new_ranker()
        r.ranker.broker = broker
        if failure == "expired":
            r.clock.advance(RUN_TIMEOUT_US + 1)
        else:

            async def invalid(_):
                raise RankingSessionError("policies missing or altered", retry=False)

            monkeypatch.setattr(r.sessions, "verify", invalid)
        assert (await r.ranker._advance(run)).startswith("failed:")
        assert broker.capabilities.get(run.run_id) is None
        assert not broker.issuance_enabled(run.run_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state,ready,expected",
    [
        ("creating", True, False),
        ("preparing", True, False),
        ("sending", False, False),
        ("sending", True, True),
        ("running", True, True),
        ("applying", True, False),
        ("done", True, False),
        ("failed", True, False),
    ],
)
async def test_ranking_gate_checks_persisted_state(service_config, state, ready, expected):
    async with rig(ranking_config(service_config)) as r:
        run = await started(r, card(1))
        await r.service.db.call(
            lambda s: rs.update_run(
                s,
                run_id=run.run_id,
                now_us=r.clock.now_utc_us(),
                state=state,
                policy_ready_at_us=r.clock.now_utc_us() + (0 if ready else 1),
            )
        )
        decision = await StoreExecutionGate(r.service.db, r.clock).token_gate(run.run_id)
        assert decision.allowed is expected
        assert decision.profile == CredentialProfile.READ_ONLY


@pytest.mark.asyncio
async def test_ranking_template_pinned_and_tampering_refused(service_config):
    async with rig(ranking_config(service_config)) as r:
        run = await started(r, card(1))
        path = service_config.state_dir / "ranking-dispatches" / f"{run.run_id}.json"
        pin = json.loads(path.read_text())
        text = r.ranker._pinned_template(run)
        assert pin == {
            "template": "ranking-v4.txt",
            "template_sha256": hashlib.sha256(text.encode()).hexdigest(),
        }
        pin["template_sha256"] = "0" * 64
        path.write_text(json.dumps(pin))
        with pytest.raises(RankingSessionError, match="pin is invalid"):
            r.new_ranker()._pinned_template(replace(run, state="sending"))


@pytest.mark.parametrize("source", ["environment", "passthrough", "unit", "none"])
def test_doctor_tokens_are_advisory_and_never_disclosed(
    service_config, tmp_path, monkeypatch, source
):
    for name in ("GITHUB_TOKEN", "GH_TOKEN", "OMNIGENT_RUNNER_ENV_PASSTHROUGH"):
        monkeypatch.delenv(name, raising=False)
    secret = "do-not-disclose-this-token"  # noqa: S105 - synthetic diagnostic sentinel
    if source == "environment":
        monkeypatch.setenv("GH_TOKEN", secret)
    if source == "passthrough":
        monkeypatch.setenv("OMNIGENT_RUNNER_ENV_PASSTHROUGH", "PATH,GITHUB_TOKEN")
    unit = f"Environment=GITHUB_TOKEN={secret}" if source == "unit" else ""

    def inspect(argv, **kwargs):
        assert argv == ["systemctl", "--user", "cat", "omni-host.service"]
        return subprocess.CompletedProcess(argv, 0, stdout=unit)

    monkeypatch.setattr(subprocess, "run", inspect)
    config = service_config.model_copy(update={"wrapper_bin_dir": tmp_path / "bin"})
    report = DoctorReport()
    _check_github_credentials(config, report)
    assert report.ok and "warning" in report.checks["gh_wrapper"]
    assert ("warning" in report.checks["runner_github_tokens"]) is (source != "none")
    assert secret not in str(report.as_dict())
    config.wrapper_bin_dir.mkdir()
    wrapper = config.wrapper_bin_dir / "gh"
    wrapper.write_text("#!/bin/sh\nexit 0\n")
    wrapper.chmod(0o700)
    report = DoctorReport()
    _check_github_credentials(config, report)
    assert report.checks["gh_wrapper"] == str(wrapper)
