# Databricks notebook source
# DBTITLE 1,Título: 00 Bronce - Ingesta
# MAGIC %md
# MAGIC # Capa Bronce — Ingesta Créditos
# MAGIC
# MAGIC **Paso 00 del pipeline medallion.** Lee el archivo crudo **BD Creditos.txt** (2.000 créditos, TXT delimitado por tabulaciones con números en formato español) y lo carga **sin transformar** a `workspace.credito_bronze.creditos_raw`.
# MAGIC
# MAGIC * **Bronce = datos crudos**: se preserva el archivo tal cual llegó, solo se renombran columnas (Delta no permite nombres con espacios) y se agregan columnas de auditoría (`_ingested_at`, `_source_file`).
# MAGIC * Las reglas de limpieza viven en el notebook **01 Silver**.
# MAGIC * Ejecución esperada: **2.000 filas, 2.000 créditos únicos**.

# COMMAND ----------

# DBTITLE 1,Explicación ingesta
# MAGIC %md
# MAGIC ## 1. Crear schemas y tabla bronce
# MAGIC
# MAGIC Se crean los tres schemas del proyecto (`credito_bronze`, `credito_silver`, `credito_gold`) y la tabla bronce. La lectura usa `read_files` directamente sobre la ruta donde quedó cargado el TXT tras subirlo al workspace.

# COMMAND ----------

# DBTITLE 1,Bronce: ingesta cruda
# CAPA BRONCE: ingesta cruda del dataset completo BD Creditos.txt (2.000 créditos, TSV)
# NOTA: la ruta idbfs:/ de un archivo adjunto al chat es TEMPORAL (~24h).
# Si el archivo ya expiró, se conserva la tabla bronce ya cargada (nunca se destruye).

source_path = 'idbfs:/2026-11-12/03/_a5130c7e-ce64-44c5-a9f9-a185a67015a2'

def fuente_disponible(ruta):
    try:
        dbutils.fs.ls(ruta)
        return True
    except Exception:
        return False

# Schemas del proyecto (idempotente)
spark.sql('CREATE SCHEMA IF NOT EXISTS workspace.credito_bronze')
spark.sql('CREATE SCHEMA IF NOT EXISTS workspace.credito_silver')
spark.sql('CREATE SCHEMA IF NOT EXISTS workspace.credito_gold')

if fuente_disponible(source_path):
    # Delta no permite espacios en nombres de columnas: se renombran (valor crudo intacto)
    spark.sql(f"""
    CREATE OR REPLACE TABLE workspace.credito_bronze.creditos_raw AS
    SELECT * EXCEPT (` ingreso_mensual `, ` monto_desembolsado `, ` saldo_capital `, ` ticket_promedio_oficina `,
                      `  desembolso_acumulado_oficina  `, `  meta_historica_oficina  `, `  meta_sugerida_oficina  `),
           ` ingreso_mensual `                 AS ingreso_mensual,
           ` monto_desembolsado `              AS monto_desembolsado,
           ` saldo_capital `                   AS saldo_capital,
           ` ticket_promedio_oficina `        AS ticket_promedio_oficina,
           `  desembolso_acumulado_oficina  ` AS desembolso_acumulado_oficina,
           `  meta_historica_oficina  `       AS meta_historica_oficina,
           `  meta_sugerida_oficina  `         AS meta_sugerida_oficina,
           current_timestamp()                AS _ingested_at,
           'BD Creditos.txt (dataset completo)' AS _source_file
    FROM read_files(
      '{source_path}',
      format => 'csv',      -- TXT delimitado por tabulaciones
      header => true,
      sep => '\t',
      schemaEvolutionMode => 'none'
    )
    """)
    print('Bronce re-ingested from the uploaded file.')
else:
    print(f'File at {source_path} is no longer available (chat uploads expire in ~24h).')
    print('Keeping the existing workspace.credito_bronze.creditos_raw (2.000 credits ingested on 2026-11-12).')
    print('To re-ingest from the file: attach BD Creditos.txt again and update source_path.')

# Validación de la ingesta (siempre se ejecuta sobre la tabla bronce)
display(spark.sql("""
SELECT COUNT(*) AS total_filas_bronce, COUNT(DISTINCT id_credito) AS creditos_unicos
FROM workspace.credito_bronze.creditos_raw
"""))