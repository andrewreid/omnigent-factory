"""Local credential broker, worktree Git wiring and the ``gh`` wrapper (architecture §6).

All processes share the owner's OS user. Everything here narrows the *normal* path and
makes it auditable (bot identity, repository-scoped short-lived tokens, refusal after a
fence); none of it is a sandbox or isolation claim.

* :mod:`.broker` - :class:`~.broker.LocalCredentialBroker`, the ``CredentialBroker`` port.
* :mod:`.capabilities` - per-stage capability secrets (hash in memory, secret in a 0600 file).
* :mod:`.server` / :mod:`.client` - the Unix-socket protocol (``SO_PEERCRED`` + capability).
* :mod:`.worktree` - daemon-owned source clone and worktree-only Git configuration.
* :mod:`.git_helper` / :mod:`.gh_wrapper` - the executables wired into a worktree.
"""
