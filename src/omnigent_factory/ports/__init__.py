"""Adapter protocols (interfaces only). Tasks 2-4 implement these; the core never does I/O.

Stable contract for later tasks:

* :class:`omnigent_factory.ports.adapter.EffectAdapter` - execute one claimed intent.
* :class:`omnigent_factory.ports.github.GitHubReader` - fresh evidence reads.
* :class:`omnigent_factory.ports.omnigent.OmnigentObserver` - adoption and tree scans.
* :class:`omnigent_factory.ports.credentials.CredentialBroker` - stage token issuance.
* :class:`omnigent_factory.ports.clock.Clock` - time.

In-memory fakes live in :mod:`omnigent_factory.testing.fakes`.
"""
