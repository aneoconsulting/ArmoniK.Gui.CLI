import pytest

from pathlib import Path


@pytest.fixture
def cmd_outputs():
    """Read command output files located in tests/outputs and return a dictionnary which keys are
    file names and values file contents."""
    output_files = [
        d
        for d in (Path(__file__).parent / "outputs").iterdir()
        if d.is_file() and d.suffix == ".txt"
    ]
    return {f.name.removesuffix(".txt"): f.open("r").read() for f in output_files}
