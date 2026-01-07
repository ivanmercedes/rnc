import os
import re
import json
import sqlite3
import requests
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Query, Body, HTTPException, Depends
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel, field_validator
from fastapi.responses import RedirectResponse
from bs4 import BeautifulSoup

# -------------------------------------------------
# Configuración
# -------------------------------------------------

DGII_RNC_URL = "https://dgii.gov.do/app/WebApps/ConsultasWeb2/ConsultasWeb/consultas/rnc.aspx"
DEFAULT_TIMEOUT = 20

CACHE_WEEKS = 1
CACHE_DELTA = timedelta(weeks=CACHE_WEEKS)

DB_PATH = "rnc_cache.sqlite"

app = FastAPI(
    title="API Consulta DGII RNC",
    version="1.1.0",
    description="Consulta RNC/Cédula DGII",
)


# -------------------------------------------------
# DB helpers
# -------------------------------------------------

def get_db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    db = get_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS rnc_cache (
            rnc TEXT PRIMARY KEY,
            response_json TEXT NOT NULL,
            created_at DATETIME NOT NULL
        )
    """)

    # Métricas agregadas
    db.execute("""
           CREATE TABLE IF NOT EXISTS usage_metrics (
               date TEXT PRIMARY KEY,
               total_requests INTEGER DEFAULT 0,
               cache_hits INTEGER DEFAULT 0,
               cache_misses INTEGER DEFAULT 0,
               errors INTEGER DEFAULT 0,
               created_at DATETIME NOT NULL,
               updated_at DATETIME NOT NULL
           )
       """)
    db.commit()
    db.close()


init_db()


# -------------------------------------------------
# Models
# -------------------------------------------------

class ConsultaRequest(BaseModel):
    rnc: str

    @field_validator("rnc", mode="before")
    @classmethod
    def sanitize(cls, v: str) -> str:
        return sanitize_rnc(v)

class ConsultaResponse(BaseModel):
    cedula_rnc: str
    nombre_razon_social: str
    nombre_comercial: str
    categoria: str
    regimen_de_pagos: str
    estado: str
    actividad_economica: str
    administracion_local: str
    facturador_electronico: str
    licencias_de_comercializacion_de_vhm: str
    rnc_consultado: str
    cache: bool
# -------------------------------------------------
# Utils
# -------------------------------------------------

def normalize_text(text: str) -> str:
    return (
        text.replace("é", "e")
        .replace("ó", "o")
        .replace("í", "i")
        .replace("á", "a")
        .replace("ú", "u")
        .replace("ñ", "n")
        .lower()
    )


def sanitize_rnc(rnc: str) -> str:
    return re.sub(r'\D', '', rnc.strip())


def rnc_param(rnc: str = Query(...)) -> str:
    return sanitize_rnc(rnc)


def parse_hidden_inputs(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    return {
        inp.get("name"): inp.get("value", "")
        for inp in soup.select("input[type=hidden]")
        if inp.get("name")
    }


def parse_result_table(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    # Mensaje genérico DGII cuando hay error de consulta
    error_span = soup.find("span", id="cphMain_lblInformacion")
    if error_span:
        mensaje = normalize_text(error_span.get_text(strip=True))

        # Caso RNC no inscrito
        if "el rnc/cedula consultado no se encuentra inscrito como contribuyente." in mensaje:
            return {"error": True, "tipo": "rnc_no_encontrado", "mensaje": mensaje}

        # Caso error de DGII (bloqueo o saturación)
        return {"error": True, "tipo": "dgii_error", "mensaje": mensaje}

    # Tabla con datos válidos
    table = soup.find("table", id=re.compile("dvDatosContribuyentes", re.I))
    data = {}
    if table:
        for tr in table.find_all("tr"):
            cols = tr.find_all("td")
            if len(cols) == 2:
                key = normalize_text(
                    cols[0]
                    .get_text(strip=True)
                    .replace(":", "")
                    .replace("/", "_")
                    .replace(" ", "_")
                )
                data[key] = cols[1].get_text(strip=True)

    if not data:
        return {"error": True, "tipo": "dgii_error", "mensaje": "Respuesta vacía de DGII"}

    return data



# -------------------------------------------------
# Cache logic
# -------------------------------------------------

def get_cached_rnc(rnc: str) -> Optional[dict]:
    db = get_db()
    row = db.execute(
        "SELECT response_json, created_at FROM rnc_cache WHERE rnc = ?",
        (rnc,),
    ).fetchone()
    db.close()

    if not row:
        return None

    response_json, created_at = row
    created_at = datetime.fromisoformat(created_at)

    if datetime.utcnow() - created_at <= CACHE_DELTA:
        data = json.loads(response_json)
        data["cache"] = True
        return data

    return None


def update_metrics(
        *,
        cache_hit: bool,
        error: bool
):
    today = datetime.utcnow().date().isoformat()
    now = datetime.utcnow().isoformat()

    db = get_db()

    # Crea la fila del día si no existe
    db.execute(
        """
        INSERT OR IGNORE INTO usage_metrics
        (date, created_at, updated_at)
        VALUES (?, ?, ?)
        """,
        (today, now, now),
    )

    # Actualiza contadores
    db.execute(
        """
        UPDATE usage_metrics
        SET
            total_requests = total_requests + 1,
            cache_hits = cache_hits + ?,
            cache_misses = cache_misses + ?,
            errors = errors + ?,
            updated_at = ?
        WHERE date = ?
        """,
        (
            1 if cache_hit else 0,
            0 if cache_hit else 1,
            1 if error else 0,
            now,
            today,
        ),
    )

    db.commit()
    db.close()


def save_cache(rnc: str, data: dict):
    # No guardar cache si el resultado es vacío o contiene error
    if not data or data.get("error"):
        return

    db = get_db()
    db.execute(
        """
        INSERT OR REPLACE INTO rnc_cache (rnc, response_json, created_at)
        VALUES (?, ?, ?)
        """,
        (
            rnc,
            json.dumps(data, ensure_ascii=False),
            datetime.utcnow().isoformat(),
        ),
    )
    db.commit()
    db.close()


# -------------------------------------------------
# DGII consulta
# -------------------------------------------------

def consulta_rnc(rnc_value: str) -> dict:
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (ConsultaRNC/1.1)",
        "Accept-Language": "es-ES,es;q=0.9",
    }

    r = session.get(DGII_RNC_URL, headers=headers, timeout=DEFAULT_TIMEOUT)
    hidden = parse_hidden_inputs(r.text)

    payload = hidden.copy()
    payload["ctl00$cphMain$txtRNCCedula"] = rnc_value
    payload["ctl00$cphMain$btnBuscarPorRNC"] = "BUSCAR"

    r2 = session.post(DGII_RNC_URL, data=payload, headers=headers, timeout=DEFAULT_TIMEOUT)

    data = parse_result_table(r2.text)
    data["rnc_consultado"] = rnc_value
    # data["cache"] = False
    return data


# -------------------------------------------------
# API
# -------------------------------------------------
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")

@app.get("/api/consulta", response_model=ConsultaResponse)
def consulta_get(rnc: str = Depends(sanitize_rnc)):
    cached = get_cached_rnc(rnc)
    if cached:
        update_metrics(cache_hit=True, error=False)
        return cached

    result = consulta_rnc(rnc)

    if result.get("error"):
        error_type = result.get("tipo")
        if error_type == "rnc_no_encontrado":
            update_metrics(cache_hit=False, error=True)
            return JSONResponse(
                status_code=404,
                content={**result, "codigo_http": 404},
            )
        # Error temporal de DGII
        update_metrics(cache_hit=False, error=True)
        return JSONResponse(
            status_code=503,
            content={**result, "codigo_http": 503},
        )

    save_cache(rnc, result)
    update_metrics(cache_hit=False, error=False)
    return result

@app.post("/api/consulta", response_model=ConsultaResponse)
def consulta_post(body: ConsultaRequest):
    rnc = body.rnc

    cached = get_cached_rnc(rnc)
    if cached:
        update_metrics(cache_hit=True, error=False)
        return cached

    result = consulta_rnc(rnc)

    if result.get("error"):
        error_type = result.get("tipo")
        if error_type == "rnc_no_encontrado":
            update_metrics(cache_hit=False, error=True)
            return JSONResponse(
                status_code=404,
                content={**result, "codigo_http": 404},
            )
        # Error temporal de DGII
        update_metrics(cache_hit=False, error=True)
        return JSONResponse(
            status_code=503,
            content={**result, "codigo_http": 503},
        )

    save_cache(rnc, result)
    update_metrics(cache_hit=False, error=False)
    return result


# @app.get("/api/stats")
# def stats():
#     db = get_db()
#     rows = db.execute(
#         """
#         SELECT
#             date,
#             total_requests,
#             cache_hits,
#             cache_misses,
#             errors
#         FROM usage_metrics
#         ORDER BY date DESC
#         """
#     ).fetchall()
#     db.close()
#
#     return [
#         {
#             "date": r[0],
#             "total_requests": r[1],
#             "cache_hits": r[2],
#             "cache_misses": r[3],
#             "errors": r[4],
#         }
#         for r in rows
#     ]
