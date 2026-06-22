# -----------------------------------------------------------------------------
# Organization: CEDA
# License: MIT
# -----------------------------------------------------------------------------
"""Pseudonimisering van gevoelige DUO-kolommen (BSN, PGN, Onderwijsnummer).

Vervangt gevoelige identifiers door een HMAC-SHA256-pseudoniem dat met een
geheime sleutel is afgeleid. Dezelfde invoer levert (met dezelfde sleutel)
hetzelfde pseudoniem op, zodat longitudinale koppeling binnen één instelling
mogelijk blijft. Zonder de sleutel is de oorspronkelijke waarde niet te
achterhalen — ook niet door de volledige BSN-ruimte te doorzoeken.

Let op: dit is *pseudonimisering*, geen anonimisering of encryptie. De
oorspronkelijke waarde is niet omkeerbaar te herstellen uit het pseudoniem.
"""

import hashlib
import hmac
import os
from typing import Any

import polars as pl
from rich.console import Console

from eencijferho.config import (
    DUO_BSN_COLUMN,
    DUO_ONDERWIJSNUMMER_COLUMN,
    DUO_PGN_COLUMN,
    get_output_dir,
)
from eencijferho.io.decorators import with_storage

ENCRYPT_KEY_ENV = "EENCIJFERHO_ENCRYPT_KEY"

# HMAC-SHA256 verwerkt sleutels langer dan zijn blokgrootte (64 bytes) door ze
# eerst te hashen; een zwakke, korte sleutel is zelf brute-force-baar en
# ondermijnt de pseudonimisering. We eisen daarom minstens de volledige
# blokgrootte aan sleutelmateriaal.
MIN_KEY_BYTES = 64


def load_key(key: str | None = None, key_file: str | None = None) -> bytes:
    """Bepaal de geheime sleutel voor pseudonimisering.

    Voorrang: expliciete ``key`` → ``key_file`` → omgevingsvariabele
    ``EENCIJFERHO_ENCRYPT_KEY``. De sleutel wordt nooit weggeschreven of gelogd.

    De sleutel moet na UTF-8-codering minstens :data:`MIN_KEY_BYTES` bytes
    bevatten; een te korte sleutel is zelf brute-force-baar.

    Raises:
        ValueError: als geen sleutel beschikbaar is, of de sleutel te kort is.
    """
    if key is None and key_file is not None:
        with open(key_file, encoding="utf-8") as fh:
            key = fh.read().strip()
    if key is None:
        key = os.environ.get(ENCRYPT_KEY_ENV)
    if not key:
        raise ValueError(
            "Geen pseudonimiseringssleutel gevonden. Geef een sleutel mee, "
            f"een sleutelbestand, of zet de omgevingsvariabele {ENCRYPT_KEY_ENV}."
        )
    encoded = key.encode("utf-8")
    if len(encoded) < MIN_KEY_BYTES:
        raise ValueError(
            f"Pseudonimiseringssleutel is te kort: {len(encoded)} bytes, "
            f"minimaal {MIN_KEY_BYTES} bytes vereist."
        )
    return encoded


def pseudonymize_value(key: bytes, value: Any) -> str | None:
    """Leid een HMAC-SHA256-pseudoniem af voor één waarde.

    ``None`` blijft ``None`` (lege cellen worden niet gepseudonimiseerd).
    """
    if value is None:
        return None
    text = str(value)
    if text == "":
        return None
    return hmac.new(key, text.encode("utf-8"), hashlib.sha256).hexdigest()


SENSITIVE_COLUMNS = (DUO_BSN_COLUMN, DUO_PGN_COLUMN, DUO_ONDERWIJSNUMMER_COLUMN)


@with_storage
def pseudonymize_files(
    storage,
    output_dir: str | None = None,
    *,
    key: str | None = None,
    key_file: str | None = None,
) -> str:
    """Vervang gevoelige kolommen in EV-/VAKHAVW-bestanden door pseudoniemen.

    De kolommen ``Burgerservicenummer``, ``Persoonsgebonden nummer`` en
    ``Onderwijsnummer`` worden ter plekke (in-place) overschreven; er blijft
    geen plaintext-identifier in de output achter en er wordt geen apart
    ``_encrypted`` bestand gemaakt.

    De sleutel wordt bepaald via :func:`load_key` en verschijnt nooit in de
    output of in deze functie's retour-log.
    """
    if output_dir is None:
        output_dir = get_output_dir()
    console = Console()
    log = ""

    secret = load_key(key=key, key_file=key_file)

    target_files: list[str] = []
    for pattern in ["EV*.csv", "VAKHAVW*.csv"]:
        target_files.extend(storage.list_files(f"{output_dir}/{pattern}"))

    def _hash(x: Any) -> str | None:
        return pseudonymize_value(secret, x)

    processed = 0
    for filepath in target_files:
        fname = os.path.basename(filepath)
        df = storage.read_dataframe(filepath, format="csv", infer_schema_length=0)
        columns_found = [c for c in SENSITIVE_COLUMNS if c in df.columns]
        if not columns_found:
            log += f"[pseudonymizer] {fname}: geen gevoelige kolommen, overgeslagen.\n"
            continue

        df = df.with_columns(
            [pl.col(c).map_elements(_hash, return_dtype=pl.Utf8).alias(c) for c in columns_found]
        )
        storage.write_text(df.write_csv(separator=";"), filepath)
        console.print(f"[green]✓[/] {fname}: {len(columns_found)} kolom(men) gepseudonimiseerd")
        log += f"[pseudonymizer] {fname}: gepseudonimiseerd: {columns_found}.\n"
        processed += 1

    log += f"[pseudonymizer] {processed} bestand(en) gepseudonimiseerd.\n"
    return log
