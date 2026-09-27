"""Git ``pre-push`` guard: a factory worktree may push only its own parcel branch.

Installed once into a daemon-owned hooks directory and selected per worktree through
``core.hooksPath`` in that worktree's own config (next to ``factory.branch``). Git hands
the hook every destination ref, so confinement is exact: no shell-text parsing. Pushing
(including force-pushing or deleting) ``refs/heads/<parcel branch>`` is allowed; any
other branch, the default branch and every tag are refused with a message saying what
to do instead. ``--no-verify`` and ``core.hooksPath`` overrides are denied by the
``factory-cel`` session policy; the GitHub ruleset still protects the default branch.
"""

from __future__ import annotations

import os
from pathlib import Path

_PRE_PUSH = """#!/bin/sh
# omnigent-factory parcel push guard (installed by the factory daemon; do not edit)
branch=$(git config --get factory.branch)
if [ -z "$branch" ]; then
  echo "factory: this worktree has no parcel branch configured; push refused." >&2
  exit 1
fi
while read -r local_ref local_sha remote_ref remote_sha; do
  [ -z "$remote_ref" ] && continue
  if [ "$remote_ref" != "refs/heads/$branch" ]; then
    echo "factory: pushing $remote_ref is not allowed from this stage; only" \\
      "refs/heads/$branch may be pushed (force-push to it is fine)." \\
      "Push the parcel branch instead, e.g. 'git push origin HEAD:$branch'." >&2
    exit 1
  fi
done
exit 0
"""


def install_push_guard(hooks_dir: Path) -> Path:
    """Write the guard into ``hooks_dir`` (0700 dir and hook) and return the directory."""
    hooks_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(hooks_dir, 0o700)
    hook = hooks_dir / "pre-push"
    tmp = hook.with_suffix(".tmp")
    tmp.write_text(_PRE_PUSH, encoding="utf-8")
    os.chmod(tmp, 0o700)  # sessions run as the daemon user
    os.replace(tmp, hook)
    return hooks_dir
