// Encrypts a chosen column in the browser via Pyodide. Inputs arrive as a
// single base64 JSON blob (__PAYLOAD_B64__) and are passed to Python as data.
// Each row gets its own random PBKDF2 salt, written to a `salt` column so the
// value can be decrypted later without a shared, source-baked salt.
const payload = JSON.parse(atob("__PAYLOAD_B64__"));
// __PYODIDE_BOOTSTRAP__
py.globals.set("csv_data_input", atob(payload.csv_b64));
py.globals.set("password_input", payload.password);
py.globals.set("column_name_input", payload.column);
const result = await py.runPythonAsync(`
import io, csv, os, base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

def derive_key(password, salt):
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                     iterations=100000, backend=default_backend())
    return base64.urlsafe_b64encode(kdf.derive(password.encode()))

rows = list(csv.DictReader(io.StringIO(csv_data_input)))
encrypted_count = 0
for row in rows:
    salt = os.urandom(16)
    row['salt'] = base64.b64encode(salt).decode()
    if column_name_input in row and row[column_name_input]:
        cipher = Fernet(derive_key(password_input, salt))
        row[column_name_input] = cipher.encrypt(row[column_name_input].encode()).decode()
        encrypted_count += 1
output = io.StringIO()
if rows:
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
import json as _json
_json.dumps({"encrypted_count": encrypted_count, "csv": output.getvalue()})
`);
return result;
