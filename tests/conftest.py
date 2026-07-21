# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# conftest.py — Fixtures et PYTHONPATH partagés pour tous les tests
# =======================================================================

from __future__ import annotations

import sys
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parent.parent
PLUGINS = ROOT / "airflow" / "plugins"

for path in (str(ROOT), str(PLUGINS)):
    if path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture
def sample_xls_content():
    # Test sample XLS content
    header = (
        "Périmètre\tNature\tDate\tHeures\tConsommation\t"
        "Prévision J-1\tPrévision J\tFioul\tCharbon\tGaz\t"
        "Nucléaire\tEolien\tSolaire\tHydraulique\tPompage\t"
        "Bioénergies\tEch. physiques\tTaux de Co2\t"
        "Ech. comm. Angleterre\tEch. comm. Espagne\t"
        "Ech. comm. Italie\tEch. comm. Suisse\t"
        "Ech. comm. Allemagne-Belgique\t"
        "Fioul - TAC\tFioul - Cogén.\tFioul - Autres\t"
        "Gaz - TAC\tGaz - Cogén.\tGaz - CCG\tGaz - Autres\t"
        "Hydraulique - Fil de l?eau + éclusée\tHydraulique - Lacs\t"
        "Hydraulique - STEP turbinage\t"
        "Bioénergies - Déchets\tBioénergies - Biomasse\tBioénergies - Biogaz\t"
        " Stockage batterie\tDéstockage batterie\t"
        "Eolien terrestre\tEolien offshore"
    )
    rows = [
        "France\tDonnées définitives\t2024-01-01\t00:00\t55239\t55000\t54200\t"
        "96\t18\t1975\t39886\t15557\t0\t6671\t-2274\t1211\t-7824\t20\t"
        "-1447\t-2\t800\t-1200\t-4000\t10\t50\t36\t500\t800\t675\t0\t"
        "3500\t2000\t1171\t500\t600\t111\t0\t0\t14000\t1557",

        "France\tDonnées définitives\t2024-01-01\t00:30\t55167\t53600\t52600\t"
        "98\t17\t1926\t38086\t15467\t0\t6533\t-2174\t1217\t-5890\t20\t"
        "-1447\t-2\t800\t-1200\t-4000\t10\t50\t38\t500\t800\t626\t0\t"
        "3400\t1900\t1233\t500\t600\t117\t0\t0\t13900\t1567",

        "France\tDonnées définitives\t2024-01-01\t01:00\t52100\t51500\t50800\t"
        "95\t15\t1800\t37500\t14800\t0\t6200\t-2000\t1100\t-5500\t18\t"
        "-1400\t-1\t750\t-1100\t-3800\t8\t48\t39\t480\t780\t540\t0\t"
        "3200\t1800\t1200\t490\t580\t130\t0\t0\t13500\t1300",
    ]
    return (header + "\n" + "\n".join(rows) + "\n").encode("latin-1")


@pytest.fixture
def sample_tempo_content():
    # Test sample Tempo content
    content = "Date\tType de jour TEMPO\n"
    content += "2024-01-01\tBLEU\n"
    content += "2024-01-02\tBLEU\n"
    content += "2024-01-15\tBLANC\n"
    content += "2024-02-01\tROUGE\n"
    return content.encode("latin-1")


@pytest.fixture
def tmp_data_dir(tmp_path, sample_xls_content, sample_tempo_content):
    # Test temporary data directory
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    xls_file = raw_dir / "eCO2mix_RTE_Annuel-Definitif_2024.xls"
    xls_file.write_bytes(sample_xls_content)

    tempo_file = raw_dir / "eCO2mix_RTE_tempo_2024-2025.xls"
    tempo_file.write_bytes(sample_tempo_content)

    return raw_dir
