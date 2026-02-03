import datetime as dt
import json
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

INICIO = "2018-01-01"  # En qué fecha inicia la serie
URL_GRAFICA = "https://www.ine.gob.bo/referencia2017/graf_trim.html"


def listar_trimestres(inicio=INICIO):
    today = dt.datetime.now().date()
    trimestres = pd.date_range(inicio, today, freq="QE")
    return trimestres


def descargar_datos(url=URL_GRAFICA):
    r = requests.get(url)
    r.raise_for_status()
    html = BeautifulSoup(r.text, "html.parser")
    script = str(
        [str(s) for s in html.select("script") if "fullData" in str(s)][0]
    )  # Encontrar el script que mencione la variable fullData
    return script


def _extract_object(script, varname):
    pattern = rf"(?:const|let|var)\s+{re.escape(varname)}\s*=\s*(\{{.*?\}});"
    match = re.search(pattern, script, re.DOTALL)
    return json.loads(re.sub(r"(\w+):", r'"\1":', match.group(1)).replace("'", '"'))


def crecimiento_nacional(script, trimestres):
    df = pd.DataFrame(_extract_object(script, "fullData"))
    df["fecha"] = trimestres[: df.shape[0]]
    df = df[["fecha", "values"]]
    df["values"] = df["values"].astype(float)
    df = df.rename(columns={"values": "crecimiento_trimestral"})
    return df


def crecimiento_actividades(script, trimestres):
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

    df = pd.DataFrame(_extract_object(str(script), "contributionsActivity"))
    df.columns = [c.encode("latin-1").decode("utf-8") for c in df.columns]
    df.index = trimestres[: df.shape[0]]
    df = df.stack().reset_index()
    df.columns = ["fecha", "actividad", "crecimiento_trimestral"]
    df.crecimiento_trimestral = df.crecimiento_trimestral.astype(float)
    df.actividad = df.actividad.map(nombres_actividades)
    return df


def actualizar_pib_graficos():
    trimestres = listar_trimestres()
    script = descargar_datos()
    nacional = crecimiento_nacional(script, trimestres)
    actividades = crecimiento_actividades(script, trimestres)
    return [
        {
            "name": "crecimiento_trimestral_nacional",
            "data": nacional,
            "keys": ["fecha"],
        },
        {
            "name": "crecimiento_trimestral_actividades",
            "data": actividades,
            "keys": ["fecha", "actividad"],
        },
    ]
