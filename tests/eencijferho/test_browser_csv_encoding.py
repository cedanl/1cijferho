"""The browser scripts must hand the uploaded CSV to Pyodide as the text that was uploaded.

The pages send the CSV as base64 of its UTF-8 bytes. atob() alone returns one
character per byte, so a name like "Öztürk" was garbled before it was encrypted
and stored.

These tests run the real page scripts in Node with a stand-in for Pyodide and
check the CSV text that the script passes to Python.
"""

import base64
import json
import shutil
import subprocess
from pathlib import Path

import pytest

_JS_DIR = Path(__file__).resolve().parents[2] / "src" / "frontend" / "Files" / "js"

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None,
    reason="node is needed to run the browser scripts",
)

# Stand-in for the browser: Pyodide is replaced by an object that records what
# the page script hands to Python. The page script uses `return`, so it runs
# inside a function, the way streamlit_js runs it.
_HARNESS = r"""
const captured = {};
globalThis.window = {
    __pyodidePromise: Promise.resolve({
        globals: { set: (name, value) => { captured[name] = value; } },
        runPythonAsync: async () => "{}",
    }),
};
const pageScript = async () => {
__PAGE_SCRIPT__
};
await pageScript();
// Escape everything outside ASCII: the result must not depend on the console encoding.
const escapeNonAscii = (ch) => "\\u" + ch.charCodeAt(0).toString(16).padStart(4, "0");
process.stdout.write(JSON.stringify(captured).replace(/[\u0080-\uffff]/g, escapeNonAscii));
"""


def _page_script(name: str, payload: dict) -> str:
    """Build the script the way the pages do (_load_js and the JSON payload)."""
    template = (_JS_DIR / name).read_text(encoding="utf-8")
    bootstrap = (_JS_DIR / "_pyodide_bootstrap.js").read_text(encoding="utf-8")
    payload_b64 = base64.b64encode(json.dumps(payload).encode()).decode()
    return template.replace("// __PYODIDE_BOOTSTRAP__", bootstrap).replace(
        "__PAYLOAD_B64__", payload_b64
    )


def _values_handed_to_python(tmp_path: Path, name: str, payload: dict) -> dict:
    """Run a page script in Node and return what it set as Python globals."""
    harness = tmp_path / "harness.mjs"
    harness.write_text(
        _HARNESS.replace("__PAGE_SCRIPT__", _page_script(name, payload)),
        encoding="utf-8",
    )
    completed = subprocess.run(
        ["node", str(harness)], capture_output=True, timeout=60, check=False
    )
    assert completed.returncode == 0, completed.stderr.decode("utf-8", "replace")
    return json.loads(completed.stdout.decode("ascii"))


@pytest.mark.parametrize(
    ("script", "other_inputs"),
    [
        ("encrypt_upload.js", {"password": "geheim", "column": "bsn"}),
        (
            "upload_personal.js",
            {"password": "geheim", "sensitive": ["bsn"], "regular": []},
        ),
    ],
)
def test_csv_with_non_ascii_text_reaches_python_unchanged(
    tmp_path, script, other_inputs
):
    csv_text = "achternaam,bsn\nÖztürk,123456782\nPeña,111222333\nŁukasz €,987654321\n"
    csv_b64 = base64.b64encode(csv_text.encode()).decode()

    handed_over = _values_handed_to_python(
        tmp_path, script, {"csv_b64": csv_b64, **other_inputs}
    )

    assert handed_over["csv_data_input"] == (
        "achternaam,bsn\nÖztürk,123456782\nPeña,111222333\nŁukasz €,987654321\n"
    )
