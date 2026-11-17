# Databricks notebook source
# DBTITLE 1,Título: 03 EDA y Validación
# MAGIC %md
# MAGIC # 03 EDA y Validación — Créditos
# MAGIC
# MAGIC **Paso 03 del pipeline medallion.** Exploración de la capa silver `workspace.credito_silver.creditos_clientes`: muestra de clientes, calidad de datos (nulos, rangos) y distribuciones clave (colocación por tipo de crédito, riesgo de mora, perfil demográfico y cobertura geográfica).
# MAGIC
# MAGIC Las capas se construyen en los notebooks anteriores:
# MAGIC
# MAGIC * **00 Bronce** — ingesta cruda (`workspace.credito_bronze.creditos_raw`)
# MAGIC * **01 Silver** — limpieza y tipado (`workspace.credito_silver.creditos_clientes`)
# MAGIC * **02 Gold** — métricas de negocio (`workspace.credito_gold.*`) y base del dashboard y Power BI

# COMMAND ----------

# DBTITLE 1,Silver: muestra de clientes
# MUESTRA DE CLIENTES (silver)
df_silver = spark.table('workspace.credito_silver.creditos_clientes')
print(f"Créditos silver: {df_silver.count()} | Clientes únicos (cédula): {df_silver.select('cedula').distinct().count()}")

muestra_clientes = (
    df_silver.sample(withReplacement=False, fraction=0.05, seed=42)  # muestra ~100 de 2.000 créditos
    .select('id_credito', 'cedula', 'tipo_credito', 'municipio', 'zona', 'edad', 'genero',
            'estado_civil', 'estrato', 'nivel_educativo', 'ingreso_mensual',
            'monto_desembolsado', 'saldo_capital', 'plazo_meses', 'dias_mora',
            'estado_credito', 'score_crediticio')
    .orderBy('id_credito')
)
display(muestra_clientes)

# COMMAND ----------

# DBTITLE 1,EDA: calidad y descriptivos (PySpark)
# EDA: calidad de datos, nulos y estadísticas descriptivas (PySpark)
from pyspark.sql import functions as F

df = spark.table('workspace.credito_silver.creditos_clientes')

# Conteo de nulos por columna clave
def nulos(nombre_col):
    return F.sum(F.col(nombre_col).isNull().cast('int'))

display(df.agg(
    F.count('*').alias('filas'),
    F.countDistinct('cedula').alias('clientes_unicos'),
    nulos('id_credito').alias('id_credito_nulos'),
    nulos('cedula').alias('cedula_nulos'),
    nulos('monto_desembolsado').alias('monto_nulos'),
    nulos('saldo_capital').alias('saldo_nulos'),
    nulos('dias_mora').alias('dias_mora_nulos'),
    nulos('score_crediticio').alias('score_nulos'),
    F.round(F.avg('edad'), 1).alias('edad_prom'),
    F.min('edad').alias('edad_min'), F.max('edad').alias('edad_max'),
    F.round(F.avg('ingreso_mensual'), 0).alias('ingreso_prom'),
    F.min('ingreso_mensual').alias('ingreso_min'), F.max('ingreso_mensual').alias('ingreso_max'),
    F.round(F.avg('monto_desembolsado'), 0).alias('monto_prom'),
    F.min('monto_desembolsado').alias('monto_min'), F.max('monto_desembolsado').alias('monto_max'),
    F.round(F.avg('score_crediticio'), 0).alias('score_prom'),
    F.min('score_crediticio').alias('score_min'), F.max('score_crediticio').alias('score_max'),
    F.round(F.avg('dias_mora'), 1).alias('dias_mora_prom'),
    F.max('dias_mora').alias('dias_mora_max'),
    F.countDistinct('codigo_oficina').alias('oficinas_distintas'),
    F.countDistinct('zona').alias('zonas_distintas'),
))

# COMMAND ----------

# DBTITLE 1,EDA: distribuciones
# EDA: distribuciones clave (colocación, mora, perfil del cliente)
from pyspark.sql import functions as F

df = spark.table('workspace.credito_silver.creditos_clientes')

# 1. Colocación por tipo de crédito
display(df.groupBy('tipo_credito')
        .agg(F.count('*').alias('creditos'),
             F.round(F.sum('monto_desembolsado') / 1e6, 1).alias('monto_MM'),
             F.round(F.avg('monto_desembolsado') / 1e6, 2).alias('ticket_prom_MM'),
             F.round(F.avg('tasa_interes_ea_pct'), 1).alias('tasa_EA_pct'))
        .orderBy(F.desc('monto_MM')))

# 2. Riesgo de mora: distribución por rango
display(df.groupBy('rango_mora')
        .agg(F.count('*').alias('creditos'),
             F.round(F.sum('saldo_capital') / 1e6, 1).alias('saldo_MM'),
             F.round(F.avg('dias_mora'), 1).alias('dias_prom'),
             F.round(F.avg('score_crediticio'), 0).alias('score_prom'))
        .orderBy('rango_mora'))

# 3. Perfil demográfico: género, estado civil y edad promedio
display(df.groupBy('genero', 'estado_civil')
        .agg(F.count('*').alias('creditos'), F.round(F.avg('edad'), 1).alias('edad_prom'),
             F.round(F.avg('ingreso_mensual') / 1e6, 2).alias('ingreso_prom_MM'))
        .orderBy('genero', 'estado_civil'))

# 4. Cobertura geográfica por municipio y zona
display(df.groupBy('municipio', 'zona')
        .agg(F.count('*').alias('creditos'),
             F.round(F.sum('monto_desembolsado') / 1e6, 1).alias('monto_MM'),
             F.round(F.sum('saldo_capital') / 1e6, 1).alias('saldo_MM'),
             F.round(100 * F.sum('flag_en_mora') / F.count('*'), 1).alias('pct_mora'))
        .orderBy(F.desc('monto_MM')))