#!/usr/bin/env python3

import argparse
from pathlib import Path
# from time import sleep

from pib_html import actualizar_pib_graficos
from pib_excel import actualizar_pib_excel


# def guardar_supabase(sb, df, tabla):
#     print("Guardando ...")
#     chunk_size = 5000
#     sleep_s = 0.2

#     n = len(df)
#     df = df.copy()
#     df.fecha = df.fecha.dt.strftime("%Y-%m-%d")
#     for i in range(0, n, chunk_size):
#         print(f"{tabla}: {n if i + chunk_size > n else i + chunk_size} filas")
#         chunk = df.iloc[i : i + chunk_size]
#         sb.table(tabla).insert(chunk.to_dict(orient="records")).execute()
#         sleep(sleep_s)


def main():
    prefix = "ine_ipc"
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
    datos = {**graficos, **excels}

    outdir = default_outdir
    outdir.mkdir(parents=True, exist_ok=True)

    for dataset in datos.keys():
        datos[dataset].to_csv(outdir / f"{dataset}.csv", index=False)

    if not args.upload:
        return

    # from supabase import create_client

    # sb_url = os.environ["SUPABASE_URL"]
    # sb_key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    # sb = create_client(sb_url, sb_key)
    # guardar_supabase(sb, nacional, "ine_pib_crecimiento_trimestral_nacional")
    # guardar_supabase(sb, actividades, "ine_pib_crecimiento_trimestral_actividades")


if __name__ == "__main__":
    main()
