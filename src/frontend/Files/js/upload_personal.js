// Encrypts sensitive columns and derives a UUID, entirely in the browser via
// Pyodide. All inputs arrive as a single base64-encoded JSON blob
// (__PAYLOAD_B64__) and are handed to Python as data via globals.set — nothing
// from user input is ever interpolated into source code.
const payload = JSON.parse(atob("__PAYLOAD_B64__"));
__PYODIDE_BOOTSTRAP__
py.globals.set("csv_data_input", atob(payload.csv_b64));
py.globals.set("password_input", payload.password);
py.globals.set("sensitive_columns_input", payload.sensitive);
py.globals.set("regular_columns_input", payload.regular);
const result = await py.runPythonAsync(`
import io, csv, os, base64, json
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

def derive_key(password, salt):
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                     iterations=100000, backend=default_backend())
    return base64.urlsafe_b64encode(kdf.derive(password.encode()))

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

    # Fresh random salt per record; the key is derived from password + this salt.
    salt = os.urandom(16)
    cipher = Fernet(derive_key(password_input, salt))

    # The UUID is derived server-side (HMAC with a secret key that never reaches
    # the browser). We pass the value to derive from; the server discards it and
    # never stores the plaintext PGN.
    uuid_source = pgn_value if pgn_value else "row-" + str(i)
    processed_row = {'__uuid_source': uuid_source, 'salt': base64.b64encode(salt).decode()}
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
