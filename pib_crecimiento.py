#!/usr/bin/env python3

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
import datetime as dt
from supabase import create_client
import os
from time import sleep

SB_URL = os.environ["SUPABASE_URL"]
SB_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

INICIO = "2018-01-01"  # En qué fecha inicia la serie


def listar_trimestres():
    today = dt.datetime.now().date()
    trimestres = pd.date_range(INICIO, today, freq="QE")
    return trimestres


def descargar_datos():
    print("Descargando datos ...")
    url = "https://www.ine.gob.bo/referencia2017/graf_trim.html"
    r = requests.get(url)
    html = BeautifulSoup(r.text, "html.parser")
    script = str(
        [str(s) for s in html.select("script") if "fullData" in str(s)][0]
    )  # Encontrar el script que mencione la variable fullData
    return script


def extract_object(script, varname):
    pattern = rf"(?:const|let|var)\s+{re.escape(varname)}\s*=\s*(\{{.*?\}});"
    match = re.search(pattern, script, re.DOTALL)
    return json.loads(re.sub(r"(\w+):", r'"\1":', match.group(1)).replace("'", '"'))


def crecimiento_nacional(script):
    print("Extrayendo la serie de crecimiento nacional ...")
    # Extraer datos
    df = pd.DataFrame(extract_object(script, "fullData"))

    # Fechas para trimestres
    df["fecha"] = trimestres[: df.shape[0]]

    # Tipos y nombres de columnas
    df = df[["fecha", "values"]]
    df["values"] = df["values"].astype(float)
    df = df.rename(columns={"values": "crecimiento_trimestral"})

    return df


def crecimiento_actividades(script):
    print("Extrayendo la serie de crecimiento por actividad ...")
    nombres_actividades = {
        "Impuestos Netos": "Impuestos Netos",
        "Agropecuaria": "Agricultura, Ganadería, Silvicultura y Pesca",
        "Extracción": "Actividad Extractiva",
        "Industria": "Industrias Manufactureras",
        "Serv. Básicos": "Suministro de Electricidad, Agua y Recolección de Desechos",
        "Construcción": "Construcción",
        "Comercio": "Comercio",
        "Transporte": "Transporte y Comunicaciones",
        "Restaurantes y Hoteles": "Alojamiento y Servicio de Comidas y Bebidas",
        "Serv. Financieros": "Actividades Financieras, Seguros, Inmobiliarias y Profesionales",
        "Adm. Pública": "Administración Pública, Salud y Educación de No Mercado",
        "Otros Servicios": "Actividades Comunales, Sociales, Personales y Servicios Domésticos",
    }

    # extraer datos
    df = pd.DataFrame(extract_object(str(script), "contributionsActivity"))

    # arreglar nombres de actividades
    df.columns = [c.encode("latin-1").decode("utf-8") for c in df.columns]

    # Fechas para trimestres
    df.index = trimestres[: df.shape[0]]

    # Forma de la tabla
    df = df.stack().reset_index()
    df.columns = ["fecha", "actividad", "crecimiento_trimestral"]
    df.crecimiento_trimestral = df.crecimiento_trimestral.astype(float)

    # Expandir nombres de actividades según la nomenclatura oficial
    df.actividad = df.actividad.map(nombres_actividades)

    return df


def guardar_supabase(sb, df, tabla):
    print("Guardando ...")
    chunk_size = 5000
    sleep_s = 0.2

    n = len(df)
    df.fecha = df.fecha.dt.strftime("%Y-%m-%d")
    for i in range(0, n, chunk_size):
        print(f"{tabla}: {n if i + chunk_size > n else i + chunk_size} filas")
        chunk = df.iloc[i : i + chunk_size]
        sb.table(tabla).insert(chunk.to_dict(orient="records")).execute()
        sleep(sleep_s)


trimestres = listar_trimestres()
script = descargar_datos()
nacional = crecimiento_nacional(script)
actividades = crecimiento_actividades(script)

sb = create_client(SB_URL, SB_KEY)
guardar_supabase(sb, nacional, "ine_pib_crecimiento_trimestral_nacional")
guardar_supabase(sb, actividades, "ine_pib_crecimiento_trimestral_actividades")
