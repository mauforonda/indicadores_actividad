#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

from upload import upload_dataset
from pib_excel import actualizar_pib_excel
from pib_html import actualizar_pib_graficos

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))


def main():
    prefix = "ine_pib"
    base_dir = Path(__file__).resolve().parents[1]
    default_outdir = base_dir / "datos" / prefix

    parser = argparse.ArgumentParser(
        description="Descarga y guarda series trimestrales del PIB de Bolivia."
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Sube los datos a Supabase (por defecto solo guarda CSV).",
    )
    args = parser.parse_args()

    print("Actualizando datos ...")
    excels = actualizar_pib_excel()
    graficos = actualizar_pib_graficos()
    datos = graficos + excels

    outdir = default_outdir
    outdir.mkdir(parents=True, exist_ok=True)

    for dataset in datos:
        dataset["data"].to_csv(outdir / f"{dataset['name']}.csv", index=False)
        if args.upload:
            upload_dataset(
                f"{prefix}_{dataset['name']}", dataset["data"], dataset["keys"]
            )


if __name__ == "__main__":
    main()
