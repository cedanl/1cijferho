// Encrypts sensitive columns and derives a UUID, entirely in the browser via
// Pyodide. All inputs arrive as a single base64-encoded JSON blob
// (__PAYLOAD_B64__) and are handed to Python as data via globals.set — nothing
// from user input is ever interpolated into source code.
const payload = JSON.parse(atob("__PAYLOAD_B64__"));
if (!window.__pyodidePromise) {
    window.__pyodidePromise = (async () => {
        const { loadPyodide } = await import("https://cdn.jsdelivr.net/pyodide/v0.24.1/full/pyodide.mjs");
        const py = await loadPyodide();
        await py.loadPackage(["micropip"]);
        await py.runPythonAsync(`
            import micropip
            await micropip.install('cryptography')
        `);
        return py;
    })();
}
const py = await window.__pyodidePromise;
py.globals.set("csv_data_input", atob(payload.csv_b64));
py.globals.set("password_input", payload.password);
py.globals.set("uuid_key_input", payload.uuid_key);
py.globals.set("sensitive_columns_input", payload.sensitive);
py.globals.set("regular_columns_input", payload.regular);
const result = await py.runPythonAsync(`
import io, csv, os, hmac, hashlib, base64, json
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

def derive_key(password, salt):
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                     iterations=100000, backend=default_backend())
    return base64.urlsafe_b64encode(kdf.derive(password.encode()))

def generate_uuid(pgn):
    h = hmac.new(uuid_key_input.encode(), str(pgn).encode(), hashlib.sha256).hexdigest()
    return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]

sensitive_columns = list(sensitive_columns_input)
regular_columns = list(regular_columns_input)
rows = list(csv.DictReader(io.StringIO(csv_data_input)))

processed_rows = []
encrypted_count = 0
for i, row in enumerate(rows):
    pgn_value = None
    for col in row:
        if col.lower() == 'persoonsgebonden_nummer':
            pgn_value = row[col]
            break
    uuid = generate_uuid(pgn_value) if pgn_value else generate_uuid("row-" + str(i))

    # Fresh random salt per record; the key is derived from password + this salt.
    salt = os.urandom(16)
    cipher = Fernet(derive_key(password_input, salt))

    processed_row = {'uuid': uuid, 'salt': base64.b64encode(salt).decode()}
    for col in sensitive_columns:
        if col in row and row[col]:
            processed_row[col.lower()] = cipher.encrypt(str(row[col]).encode()).decode()
            encrypted_count += 1
        else:
            processed_row[col.lower()] = ''
    for col in regular_columns:
        processed_row[col.lower()] = row.get(col, '')
    for col in row:
        if col not in sensitive_columns and col not in regular_columns:
            processed_row[col] = row[col]
    processed_rows.append(processed_row)

json.dumps({"encrypted_count": encrypted_count, "rows": processed_rows})
`);
return result;
