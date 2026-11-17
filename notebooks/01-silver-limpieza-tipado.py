# Databricks notebook source
# DBTITLE 1,Título: 01 Silver - Limpieza
# MAGIC %md
# MAGIC # Capa Silver — Limpieza y Tipado
# MAGIC
# MAGIC **Paso 01 del pipeline medallion.** Toma la tabla bronce cruda `workspace.credito_bronze.creditos_raw` y produce `workspace.credito_silver.creditos_clientes`: una tabla **limpia, tipada y enriquecida**, lista para análisis.
# MAGIC
# MAGIC Transformaciones clave:
# MAGIC
# MAGIC * **Números en formato español**: el TXT usa coma decimal y punto de miles (`1.250.000,50`). Se normalizan con `replace(regexp_replace(trim(...), '[.]', ''), ',', '.')` antes de convertir a `DECIMAL`.
# MAGIC * **Fechas**: `fecha_desembolso` viene como número de serie Excel → se convierte con `date_add(DATE '1899-12-30', ...)`.
# MAGIC * **Deduplicación**: se conserva un único registro por `id_credito` (el de mayor `pagare`).
# MAGIC * **Enriquecimiento**: se derivan `rango_mora` (buckets de mora) y `flag_en_mora` (0/1) para el análisis de riesgo.
# MAGIC * **Estandarización**: `trim()` en todos los campos de texto.
# MAGIC
# MAGIC Ejecución esperada: **2.000 filas, 2.000 créditos únicos, 1.900 clientes únicos, fechas jun–oct 2026**.

# COMMAND ----------

# DBTITLE 1,Silver: limpieza y tipado (PySpark)
# CAPA SILVER: limpieza, tipado, estandarización y enriquecimiento (PySpark)
from pyspark.sql import functions as F, Window

bronce = spark.table('workspace.credito_bronze.creditos_raw')

# 1) DEDUPLICACIÓN: un único registro por id_credito (el de mayor pagaré)
ventana = Window.partitionBy('id_credito').orderBy(F.col('pagare').desc())
bronce_dedup = (
    bronce
    .withColumn('_rn', F.row_number().over(ventana))
    .filter(F.col('_rn') == 1)
    .drop('_rn')
)

# 2) Números en formato español: quitar punto de miles y coma → punto decimal
def numero_es(nombre_col, tipo='decimal(18,2)'):
    limpio = F.regexp_replace(F.trim(F.col(nombre_col).cast('string')), '[.]', '')  # quita miles
    limpio = F.regexp_replace(limpio, ',', '.')                                    # coma → punto
    return limpio.cast(tipo)

# 3) SELECT tipado + columnas derivadas
silver = bronce_dedup.select(
    'id_credito',
    F.col('cedula').cast('string').alias('cedula'),
    'pagare',
    # fecha_desembolso viene como número de serie Excel -> fecha (base 1899-12-30)
    (F.lit('1899-12-30').cast('date') + F.col('fecha_desembolso').cast('int')).alias('fecha_desembolso'),
    'id_tipo_credito',
    F.trim('tipo_credito').alias('tipo_credito'),
    F.trim('ocupacion').alias('ocupacion'),
    'codigo_oficina',
    F.trim('zona').alias('zona'),
    F.trim('municipio').alias('municipio'),
    F.trim('departamento').alias('departamento'),
    'edad',
    F.trim('genero').alias('genero'),
    F.trim('estado_civil').alias('estado_civil'),
    'estrato',
    F.trim('nivel_educativo').alias('nivel_educativo'),
    numero_es('ingreso_mensual').alias('ingreso_mensual'),
    'antiguedad_cliente_anios',
    numero_es('monto_desembolsado').alias('monto_desembolsado'),
    numero_es('saldo_capital').alias('saldo_capital'),
    'plazo_meses',
    numero_es('tasa_interes_ea_pct', 'decimal(5,2)').alias('tasa_interes_ea_pct'),
    'score_crediticio',
    'dias_mora',
    F.trim('estado_credito').alias('estado_credito'),
    # Enriquecimiento: bucket de mora y flag binaria
    F.when(F.col('dias_mora') == 0, 'Sin mora')
     .when(F.col('dias_mora').between(1, 30), '1-30 dias')
     .when(F.col('dias_mora').between(31, 60), '31-60 dias')
     .when(F.col('dias_mora').between(61, 90), '61-90 dias')
     .otherwise('90+ dias').alias('rango_mora'),
    F.when(F.col('dias_mora') > 0, 1).otherwise(0).alias('flag_en_mora'),
    'mercado_potencial_zona', 'ingreso_promedio_zona', 'tasa_desempleo_zona_pct',
    'indice_educativo_zona', 'numero_asesores', 'clientes_activos_oficina',
    'clientes_nuevos_periodo',
    numero_es('penetracion_mercado_pct', 'decimal(10,4)').alias('penetracion_mercado_pct'),
    numero_es('participacion_oficina_pct', 'decimal(10,4)').alias('participacion_oficina_pct'),
    numero_es('ticket_promedio_oficina').alias('ticket_promedio_oficina'),
    numero_es('desembolso_acumulado_oficina').alias('desembolso_acumulado_oficina'),
    numero_es('meta_historica_oficina').alias('meta_historica_oficina'),
    numero_es('cumplimiento_meta_pct', 'decimal(10,2)').alias('cumplimiento_meta_pct'),
    numero_es('meta_sugerida_oficina').alias('meta_sugerida_oficina'),
    F.current_timestamp().alias('_processed_at'),
)

# 4) Escribir la tabla silver
silver.write.mode('overwrite').saveAsTable('workspace.credito_silver.creditos_clientes')

# 5) Validación
silver = spark.table('workspace.credito_silver.creditos_clientes')
display(silver.agg(
    F.count('*').alias('filas_silver'),
    F.countDistinct('id_credito').alias('creditos_unicos'),
    F.countDistinct('cedula').alias('clientes_unicos'),
    F.min('fecha_desembolso').alias('fecha_min'),
    F.max('fecha_desembolso').alias('fecha_max'),
))