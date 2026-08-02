from pathlib import Path


README = Path(__file__).resolve().parents[1] / "README.md"
HOSTED_ACTIVATION_EXECUTABLE = (
    "~/.local/share/agentsor-file/venv/bin/agentsor-file"
)


def test_readme_runtime_path_matches_hosted_activation_handoff() -> None:
    """Keep the direct GitHub path compatible with the one-time handoff."""

    text = README.read_text(encoding="utf-8")
    quickstart = text.split("## Hosted deadline reporting", maxsplit=1)[0]

    assert "python3 -m venv ~/.local/share/agentsor-file/venv" in quickstart
    assert (
        "~/.local/share/agentsor-file/venv/bin/python -m pip install "
        "agentsor-file==0.2.4"
    ) in quickstart
    for command in ("init", "check", "report"):
        assert f"{HOSTED_ACTIVATION_EXECUTABLE} {command}" in text

    assert "python3 -m venv .venv" not in quickstart
    assert ". .venv/bin/activate" not in quickstart
