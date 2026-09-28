# Tests voor de demodata in data/01-input/DEMO.
#
# De demoset bevat twee 1cyferho-bestandsbeschrijvingen: eentje voor 2023 en
# de kopie voor 2024. De EV-dataset in de demoset is `EV299XX24_DEMO.asc`, dus
# uit 2024, en de jaargangmatching uit issue #198 kiest dan de 2024-versie. De
# 2023-versie blijft staan zodat er iets is om mee te vergelijken.
#
# De kopie bestaat alleen om de demo zonder escape hatch te kunnen draaien; de
# layout is bewust hetzelfde. Daarom moeten ze gelijk blijven: als iemand de
# ene aanpast en de andere vergeet, draait de demo stilletjes tegen een
# verouderde layout. Deze test maakt dat verschil zichtbaar.

from pathlib import Path

import pytest

DEMO_DIR = Path(__file__).resolve().parents[1] / "data" / "01-input" / "DEMO"
BB_2023 = DEMO_DIR / "Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO.txt"
BB_2024 = DEMO_DIR / "Bestandsbeschrijving_1cyferho_2024_v1.1_DEMO.txt"


def _read_lines(path: Path) -> list[str]:
    return path.read_text(encoding="latin-1").splitlines()


def test_demo_bb_2023_bestaat():
    assert BB_2023.exists()


def test_demo_bb_2024_bestaat():
    assert BB_2024.exists()


def test_demo_bb_kopie_is_identiek_op_naam_na():
    """Regel 1 beschrijft de eigen bestandsnaam en mag dus verschillen."""
    oud, nieuw = _read_lines(BB_2023), _read_lines(BB_2024)
    assert oud[1:] == nieuw[1:], (
        "De 2023- en 2024-demo-BB lopen uit elkaar. Pas ze samen aan, anders "
        "gebruikt de demo stilletjes een verouderde layout."
    )


def test_demo_bb_2024_noemt_zichzelf_2024():
    assert "2024" in _read_lines(BB_2024)[0]


def test_demo_ev_bestand_is_uit_2024():
    assert (DEMO_DIR / "EV299XX24_DEMO.asc").exists()
