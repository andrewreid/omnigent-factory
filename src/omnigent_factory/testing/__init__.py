"""In-memory fakes, a fake clock and fixture builders for tests of every task.

No real network. Import from here rather than re-creating fixtures per task:

* :mod:`omnigent_factory.testing.fakes` - FakeClock, FakeGitHub, FakeOmnigent,
  FakeCredentialBroker, FakeScheduler.
* :mod:`omnigent_factory.testing.builders` - config/snapshot/contract builders and
  :class:`~omnigent_factory.testing.builders.EventFactory`.
* :mod:`omnigent_factory.testing.harness` - an in-memory multi-parcel reducer harness
  with scripted flows (triage, plan, approve, admit, build ready...).
"""
