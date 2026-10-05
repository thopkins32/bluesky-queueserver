import subprocess
import sys


def test_v2_import_does_not_load_heavy_or_legacy_modules():
    code = """
import sys
import bluesky_queueserver.v2 as v2

assert v2.API_VERSION == "2"
for prefix in (
    "bluesky",
    "ophyd",
    "fastapi",
    "bluesky_queueserver.manager",
    "bluesky_queueserver._experiment_controller",
):
    assert not any(name == prefix or name.startswith(f"{prefix}.") for name in sys.modules), prefix
"""
    subprocess.run([sys.executable, "-c", code], check=True)
