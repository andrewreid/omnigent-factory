"""Exercise the actual CEL evaluator: abstention is never counted as ALLOW."""

import pytest
from omnigent.policies.builtins.cel import cel_policy

from omnigent_factory.omnigent import policies as pol

WRAPPER = "/factory/bin/gh"


@pytest.fixture(
    scope="module", params=[{}, {"read_only": True}, {"read_only": True, "reference": True}]
)
def policy(request):
    spec = pol.factory_cel_policy(
        gh_wrapper=WRAPPER, real_gh_path="/opt/github-cli", **request.param
    )
    return cel_policy(**spec.factory_params)


def event(value, key="command", tool="sys_os_shell"):
    return {"type": "tool_call", "data": {"name": tool, "arguments": {key: value}}}


@pytest.mark.parametrize(
    "command",
    [
        "gh issue view 1",
        "command gh issue view 1",
        "env gh issue view 1",
        r"\gh issue view 1",
        "'gh' issue view 1",
        '"gh" issue view 1',
        "g''h issue view 1",
        r"g\h issue view 1",
        "/usr/bin/gh issue view 1",
        "'/usr/bin/gh' issue view 1",
        "/bin/gh issue view 1",
        "/opt/github-cli issue view 1",
        "sh -c 'gh issue view 1'",
        "alias github='gh'; github issue view 1",
        "alias github=/usr/bin/gh",
        f"{WRAPPER} issue view 1; gh issue view 2",
        'curl -H "Authorization: Bearer $GITHUB_TOKEN" https://api.github.com',
        'echo "${GH_TOKEN}"',
        "printenv GITHUB_TOKEN",
        "cat ~/.config/gh/hosts.yml",
        'cat "$HOME/.config/gh/hosts.yml"',
        "cat /home/owner/.config/gh/config.yml",
        "cat hosts.yml",
        f"{WRAPPER} auth token",
        f"'{WRAPPER}' auth status --show-token",
    ],
)
def test_personal_identity_denied(policy, command):
    result = policy(event(command))
    assert result is not None and result["result"] == "DENY"
    assert WRAPPER in result["reason"]


@pytest.mark.parametrize(
    ("tool", "key", "value"),
    [
        ("exec_command", "cmd", "env gh api user"),
        ("Bash", "command", ["bash", "-lc", "gh api user"]),
        ("shell", "command", ["/usr/bin/gh", "api", "user"]),
        ("Read", "file_path", "/home/owner/.config/gh/hosts.yml"),
        ("read_file", "path", "~/.config/gh/config.yml"),
        ("read_files", "paths", ["README.md", "~/.config/gh/hosts.yml"]),
    ],
)
def test_shell_and_file_surfaces(policy, tool, key, value):
    assert policy(event(value, key, tool))["result"] == "DENY"


@pytest.mark.parametrize(
    "command",
    [
        f'OMNIGENT_FACTORY_CAPABILITY_FILE="/private/run.cap" "{WRAPPER}" issue view 1',
        f"{WRAPPER} api repos/o/r/issues/1",
        f"'{WRAPPER}' run list",
        f'command "{WRAPPER}" pr view 1',
        "git status",
        "cat README.md",
    ],
)
def test_scoped_wrapper_and_unrelated_reads_allowed(policy, command):
    assert policy(event(command))["result"] == "ALLOW"


@pytest.mark.parametrize(
    "command",
    [
        "GIT_TRACE_REDACT=0 git fetch origin main",
        'env GIT_TRACE_REDACT="0" git fetch origin main',
        "GIT_TRACE_CURL=1 git fetch origin main",
        "export GIT_CURL_VERBOSE=true; git fetch origin main",
        ["env", "GIT_TRACE_CURL=/tmp/curl.trace", "git", "fetch", "origin", "main"],
    ],
)
def test_unsafe_git_trace_environment_denied_in_every_stage(policy, command):
    result = policy(event(command))
    assert result is not None and result["result"] == "DENY"


def test_git_trace_redaction_stays_allowed_when_enabled(policy):
    assert policy(event("GIT_TRACE_REDACT=1 git status"))["result"] == "ALLOW"


@pytest.mark.parametrize(
    "command",
    [
        f"'{WRAPPER}' pr merge 1",
        f'"{WRAPPER}" issue create -t test',
        f"{WRAPPER} workflow run deploy.yml",
        "git push origin main",
    ],
)
def test_wrapper_does_not_bypass_existing_operation_denials(policy, command):
    assert policy(event(command))["result"] == "DENY"


def test_configuration_changes_policy_version():
    first = pol.factory_cel_policy(gh_wrapper=WRAPPER)
    assert first.name != pol.factory_cel_policy(gh_wrapper="/different/gh").name
    assert first.name != pol.factory_cel_policy(real_gh_path="/opt/gh").name


def test_adapter_uses_configured_paths_in_every_stage_policy(git_env):
    from tests.omnigent.support import make_rig

    wrapper = "/custom factory/bin/gh"
    real = "/opt/github-cli"
    rig = make_rig(git_env, gh_wrapper=wrapper, real_gh_path=real)
    for specs in (rig.adapter._cel, rig.adapter._cel_read_only, rig.adapter._cel_reference):
        check = cel_policy(**specs[0].factory_params)
        verdict = check(event(f"{real} api user"))
        assert verdict["result"] == "DENY" and wrapper in verdict["reason"]
        assert check(event(f'"{wrapper}" api user'))["result"] == "ALLOW"


@pytest.mark.parametrize(
    "command",
    [
        "printf 'protocol=https\\nhost=github.com\\n\\n' | git credential fill",
        'git credential fill <<<"protocol=https\\nhost=github.com"',
        "git credential approve",
        "git credential reject",
        "git -c credential.useHttpPath=true credential fill",
        "git -C /tmp -c credential.useHttpPath=true credential approve",
        "git --git-dir=/tmp/repo/.git credential reject",
        "git --git-dir /tmp/repo/.git credential fill",
        "git --no-pager credential fill",
        "git \\\ncredential fill",
        '"/usr/bin/git" "credential" fill',
        "command git credential fill",
        "env git credential fill",
        "sh -c 'git credential fill'",
        "git credential-store get",
        "git -c credential.useHttpPath=true credential-cache get",
        "git credential-manager get",
        "git-credential-store get",
        "/usr/lib/git-core/git-credential-cache get",
        "/factory/bin/git-credential-omnigent-factory get",
        f"{WRAPPER} auth git-credential",
        "git -c alias.creds='credential fill' creds",
        'git -c "alias.creds=credential approve" creds',
        "git -c alias.creds=credential-store creds get",
        "git -c alias.creds='!git credential fill' creds",
        "alias creds='git credential fill'; creds",
        ["bash", "-lc", "git credential fill"],
        ["git", "credential", "fill"],
        ["env", "-i", "/usr/bin/git", "credential", "fill"],
        ["command", "git", "credential", "reject"],
        ["/usr/bin/git", "-c", "credential.useHttpPath=true", "credential", "approve"],
        ["git", "--git-dir=/tmp/repo/.git", "credential", "reject"],
        ["git", "credential-store", "get"],
        ["git-credential-store", "get"],
        ["git", "-c", "alias.creds=credential fill", "creds"],
    ],
)
def test_explicit_git_credential_access_denied_in_every_stage(policy, command):
    result = policy(event(command))
    assert result is not None and result["result"] == "DENY"


@pytest.mark.parametrize(
    "command",
    [
        "git push origin HEAD:factory/issue-1",
        "git -C /factory/worktree push origin factory/issue-1",
        "git fetch origin main",
        ["git", "push", "origin", "HEAD:factory/issue-1"],
        ["git", "fetch", "origin", "main"],
        ["env", "git", "push", "origin", "HEAD:factory/issue-1"],
        "git show HEAD:src/credentials.py",
    ],
)
@pytest.mark.parametrize("read_only", [False, True])
def test_implicit_git_helpers_still_allowed(command, read_only):
    # The reference variant already denies network Git; retain that separate rule.
    check = cel_policy(**pol.factory_cel_policy(read_only=read_only).factory_params)
    assert check(event(command))["result"] == "ALLOW"


@pytest.mark.parametrize("value", [None, 42, {}, [], [None], ["git", None], ["git", {}]])
def test_credential_argv_guard_does_not_abstain_on_malformed_values(policy, value):
    assert policy(event(value))["result"] == "ALLOW"


@pytest.mark.parametrize("key", ["cmd", "script"])
def test_split_credential_argv_cannot_hide_beside_a_scalar_command(policy, key):
    call = event(42)
    call["data"]["arguments"][key] = ["git", "credential", "fill"]
    assert policy(call)["result"] == "DENY"
