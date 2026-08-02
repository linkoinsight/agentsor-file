from collections import Counter
from pathlib import Path
import re


README = Path(__file__).resolve().parents[1] / "README.md"
HOSTED_ACTIVATION_EXECUTABLE = "~/.local/share/agentsor-file/venv/bin/agentsor-file"
SHELL_SCRIPT_EXECUTABLE = '"$HOME/.local/share/agentsor-file/venv/bin/agentsor-file"'


def test_readme_runtime_path_matches_hosted_activation_handoff() -> None:
    """Keep the direct GitHub path compatible with the one-time handoff."""

    text = README.read_text(encoding="utf-8")
    quickstart = text.split("## Hosted deadline reporting", maxsplit=1)[0]

    assert "python3 -m venv ~/.local/share/agentsor-file/venv" in quickstart
    assert (
        "~/.local/share/agentsor-file/venv/bin/python -m pip install "
        "agentsor-file==0.2.5"
    ) in quickstart
    code_blocks = re.findall(r"```(?:bash|sh)\n(.*?)```", text, re.DOTALL)
    invocations: list[tuple[str, str]] = []
    for block in code_blocks:
        for line in block.splitlines():
            match = re.fullmatch(
                r"(?P<executable>"
                r"~/.local/share/agentsor-file/venv/bin/agentsor-file|"
                r'"\$HOME/.local/share/agentsor-file/venv/bin/agentsor-file"'
                r") "
                r"(?P<command>init|check|schema|report)(?: .*)?",
                line,
            )
            if match is None:
                continue
            invocations.append((match["executable"], match["command"]))

    assert Counter(command for _executable, command in invocations) == {
        "init": 1,
        "check": 1,
        "schema": 1,
        "report": 2,
    }
    assert all(
        executable in {HOSTED_ACTIVATION_EXECUTABLE, SHELL_SCRIPT_EXECUTABLE}
        for executable, _command in invocations
    )

    assert "python3 -m venv .venv" not in quickstart
    assert ". .venv/bin/activate" not in quickstart
    assert "/opt/partner-feed/.venv/bin/agentsor-file" not in text
