#!/usr/bin/env ptyhon3

import pandas as pd


def parse_table(fn, index_cols=["categoria"]):
    def drop_footer_rows(raw, min_non_nan=2):
        cleaned = raw.replace(r"^\s*$", pd.NA, regex=True)
        non_nan = cleaned.notna().sum(axis=1)
        skip = 0
        for v in reversed(non_nan.tolist()):
            if v <= min_non_nan:
                skip += 1
            else:
                break
        return raw.iloc[:-skip] if skip > 0 else raw

    index_cols_len = len(index_cols)

    raw = pd.read_excel(fn, skiprows=9, header=None).iloc[:, 1:]
    raw = drop_footer_rows(raw, min_non_nan=2)
    with pd.option_context("future.no_silent_downcasting", True):
        years = raw.iloc[0, index_cols_len:].ffill().infer_objects(copy=False)
    months = raw.iloc[1, index_cols_len:]
    data = raw.iloc[3:].copy()
    data.columns = index_cols + list(range(index_cols_len, raw.shape[1]))
    date_cols = pd.MultiIndex.from_arrays([years, months], names=["year", "month"])
    table = data.iloc[:, index_cols_len:]
    table.columns = date_cols
    table.index = pd.MultiIndex.from_frame(data.iloc[:, :index_cols_len])
    vertical = (
        table.stack([0, 1], future_stack=True)
        .reset_index(name="valor")
        .dropna(subset=["year", "month", "valor"])
    )
    vertical = vertical[vertical.categoria.notna()]
    return vertical


def clean_table(df):
    # Construir fechas
    df.year = df.year.astype(str).str.extract(r"([0-9]*)")
    df.month = (
        df.month.astype(str).str.strip().map({"I": 3, "II": 6, "III": 9, "IV": 12})
    )
    df["fecha"] = (
        pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str))
        .dt.to_period("M")
        .dt.to_timestamp("M")
    )

    # Clasificar valores en la columna de categorías como agregados, impuestos o actividades económicas
    def clasificar_categoria(categoria):
        tipos = {
            "agregados": ["producto interno bruto", "valor agregado bruto"],
            "impuestos": ["importaciones"],
        }

        categoria = categoria.lower().strip()
        for tipo in tipos.keys():
            for i in tipos[tipo]:
                if i in categoria:
                    return tipo

        return "actividad económica"

    df["tipo"] = df.categoria.apply(clasificar_categoria)
    df.categoria = df.categoria.str.strip().str.capitalize()

    # Retornar sólo las columnas relevantes

    df = df[["tipo", "categoria", "fecha", "valor"]]
    return df


def actualizar_pib_excel():
    excels = [
        {
            "nombre": "volumen_encadenado_por_actividad",
            "url": "https://www.ine.gob.bo/referencia2017/CUADROS/pagina_web/T_01.03.xlsx",
        },
        {
            "nombre": "volumen_encadenado_por_actividad_variacion",
            "url": "https://www.ine.gob.bo/referencia2017/CUADROS/pagina_web/T_01.05.xlsx",
        },
        {
            "nombre": "volumen_encadenado_por_actividad_variacion_a_12_meses",
            "url": "https://www.ine.gob.bo/referencia2017/CUADROS/pagina_web/T_01.05.02.xlsx",
        },
    ]
    data = []

    for excel in excels:
        df = parse_table(excel["url"])
        df = clean_table(df)
        data.append(
            {
                "name": excel["nombre"],
                "data": df,
                "keys": ["tipo", "categoria", "fecha"],
            }
        )

    return data
