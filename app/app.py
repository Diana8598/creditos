# -*- coding: utf-8 -*-
"""Scoring de riesgo de mora para créditos.

App que consulta el serving endpoint 'creditos' (modelo
workspace.credito_ml.modelo_riesgo_mora v1) con autenticación
on-behalf-of del usuario que la usa.
"""
from flask import Flask, render_template_string, request

app = Flask(__name__)

ENDPOINT = "/serving-endpoints/creditos/invocations"  # ruta actual: sin prefijo /api/2.0

# Opciones de los campos categóricos (mismas categorías del entrenamiento)
CATALOGOS = {
    "tipo_credito": ["Consumo", "Libranza", "Comercial", "Microcredito"],
    "ocupacion": ["Empleado", "Independiente"],
    "genero": ["M", "F"],
    "estado_civil": ["Soltero", "Casado", "Union libre", "Separado"],
    "nivel_educativo": ["Bachiller", "Tecnico", "Tecnologo", "Universitario", "Posgrado", "Basica primaria"],
    "zona": ["Envigado", "Itagui", "Belen", "La 80", "Primero de Mayo", "Colombia", "Alpujarra", "San Javier"],
}
ETIQUETAS = {
    "tipo_credito": "Tipo de crédito",
    "ocupacion": "Ocupación",
    "genero": "Género",
    "estado_civil": "Estado civil",
    "nivel_educativo": "Nivel educativo",
    "zona": "Agencia (zona)",
}
NUMERICOS = [
    ("edad", "Edad (años)"),
    ("ingreso_mensual", "Ingreso mensual (COP)"),
    ("antiguedad_cliente_anios", "Antigüedad como cliente (años)"),
    ("monto_desembolsado", "Monto a desembolsar (COP)"),
    ("plazo_meses", "Plazo (meses)"),
    ("tasa_interes_ea_pct", "Tasa de interés EA (%)"),
    ("score_crediticio", "Score crediticio"),
    ("estrato", "Estrato"),
]
DEFAULTS = {
    "edad": 38, "ingreso_mensual": 4200000, "antiguedad_cliente_anios": 6,
    "monto_desembolsado": 25000000, "plazo_meses": 36, "tasa_interes_ea_pct": 18.5,
    "score_crediticio": 698, "estrato": 3,
}


def _workspace():
    """Cliente del workspace. Databricks Apps inyecta la autenticación del
    usuario (on-behalf-of); si no está disponible usa la identidad de la app."""
    from databricks.sdk import WorkspaceClient
    try:
        from databricks.sdk.credentials_provider import ContextCredentialsProvider
        return WorkspaceClient(config={"credentials_provider": ContextCredentialsProvider})
    except Exception:
        return WorkspaceClient()


def invocar_modelo(registro):
    """Consulta el serving endpoint 'creditos' (modelo riesgo de mora v1).
    Ruta actual: /serving-endpoints/<nombre>/invocations, sin prefijo /api/2.0."""
    payload = {"dataframe_records": [registro]}
    # 1) POST directo con el token del usuario (on-behalf-of, inyectado por Apps)
    try:
        import os
        import requests
        from databricks.sdk.core import Config
        from databricks.sdk.credentials_provider import ContextCredentialsProvider
        host = os.environ.get("DATABRICKS_HOST") or Config().host
        headers = dict(ContextCredentialsProvider()())
        headers["Content-Type"] = "application/json"
        respuesta = requests.post(f"{host.rstrip('/')}{ENDPOINT}", headers=headers,
                                  json=payload, timeout=30)
        respuesta.raise_for_status()
        return respuesta.json()
    except Exception:
        # 2) Fallback: identidad de la app vía SDK (ruta catalogada)
        return _workspace().api_client.do("POST", ENDPOINT, body=payload)


PLANTILLA = """<!doctype html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Scoring de Riesgo — Créditos</title>
<style>
 body{font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;background:#f5f7f9;color:#11181c;margin:0;padding:32px 16px}
 .card{max-width:680px;margin:0 auto;background:#fff;border:1px solid #e3e8ee;border-radius:10px;padding:24px 28px}
 h1{font-size:20px;margin:0 0 4px} .sub{color:#5f7281;font-size:13px;margin:0 0 18px}
 label{display:block;font-size:12.5px;font-weight:600;margin:10px 0 4px}
 select,input{width:100%;box-sizing:border-box;padding:8px 9px;border:1px solid #cfd8e0;border-radius:6px;font-size:14px;background:#fff}
 .grid{display:grid;grid-template-columns:1fr 1fr;gap:0 16px}
 @media(max-width:560px){.grid{grid-template-columns:1fr}}
 button{margin-top:20px;background:#3b7dd8;color:#fff;border:0;border-radius:6px;padding:10px 22px;font-size:14px;cursor:pointer}
 button:hover{background:#2f66b5}
 .res{padding:16px;border-radius:8px;margin-bottom:18px;font-size:15px}
 .ok{background:#eef7f0;border:1px solid #bcd9c5}
 .riesgo{background:#fdeeee;border:1px solid #ecc7c7}
 .meta{color:#5f7281;font-size:12px;margin-top:14px}
 code{background:#f2f5f8;padding:1px 5px;border-radius:4px}
</style></head><body>
<div class="card">
 <h1>Scoring de Riesgo de Mora</h1>
 <p class="sub">Modelo <code>workspace.credito_ml.modelo_riesgo_mora v1</code> · endpoint <code>creditos</code></p>
 {% if resultado %}
   {% if error %}
   <div class="res riesgo"><b>Error al consultar el modelo:</b> {{ error }}</div>
   {% elif prediccion == 1 %}
   <div class="res riesgo"><b>&#9888; Riesgo de mora</b> — el modelo clasifica esta solicitud como <b>en mora</b>.</div>
   {% else %}
   <div class="res ok"><b>&#10003; Al día</b> — el modelo clasifica esta solicitud como <b>sin mora</b>.</div>
   {% endif %}
 {% endif %}
 <form method="post" action="/score">
   <div class="grid">
   {% for campo, opciones in catalogos.items() %}
   <div><label>{{ etiquetas[campo] }}</label>
     <select name="{{ campo }}">{% for o in opciones %}<option value="{{ o }}">{{ o }}</option>{% endfor %}</select></div>
   {% endfor %}
   {% for campo, etiqueta in numericos %}
   <div><label>{{ etiqueta }}</label>
     <input type="number" step="any" name="{{ campo }}" value="{{ valores[campo] }}" required></div>
   {% endfor %}
   </div>
   <button type="submit">Calcular riesgo</button>
 </form>
</div></body></html>"""


@app.route("/")
def index():
    return render_template_string(PLANTILLA, resultado=False, error=None,
                                  catalogos=CATALOGOS, etiquetas=ETIQUETAS,
                                  numericos=NUMERICOS, valores=DEFAULTS)


@app.route("/score", methods=["POST"])
def score():
    registro = {campo: request.form.get(campo) for campo in CATALOGOS}
    registro.update({campo: float(request.form.get(campo)) for campo, _ in NUMERICOS})
    try:
        respuesta = invocar_modelo(registro)
        preds = respuesta.get("predictions", []) if isinstance(respuesta, dict) else None
        if not preds:
            raise ValueError(f"respuesta inesperada del endpoint: {respuesta}")
        return render_template_string(PLANTILLA, resultado=True, error=None,
                                      prediccion=int(preds[0]), respuesta=respuesta,
                                      catalogos=CATALOGOS, etiquetas=ETIQUETAS,
                                      numericos=NUMERICOS, valores=registro)
    except Exception as exc:  # noqa: BLE001 - se muestra al usuario en la UI
        return render_template_string(PLANTILLA, resultado=True, error=str(exc),
                                      catalogos=CATALOGOS, etiquetas=ETIQUETAS,
                                      numericos=NUMERICOS, valores=registro)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
