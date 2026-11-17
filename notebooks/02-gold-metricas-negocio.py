# Databricks notebook source
# DBTITLE 1,Título: 02 Gold - Métricas
# MAGIC %md
# MAGIC # Capa Gold — Métricas de Negocio
# MAGIC
# MAGIC **Paso 02 del pipeline medallion.** Agrega la tabla silver `workspace.credito_silver.creditos_clientes` en dos tablas gold listas para el dashboard y Power BI:
# MAGIC
# MAGIC * **`workspace.credito_gold.metricas_agencia`** — una fila por agencia/oficina comercial (8 en total, en Antioquia) con:
# MAGIC   * **Colocación**: créditos colocados, monto colocado, ticket promedio.
# MAGIC   * **Riesgo de mora**: créditos en mora, saldo en mora, % de créditos en mora, **IMOR** (saldo en mora / saldo total), días de mora promedio y máximo.
# MAGIC   * **Productividad**: colocación por asesor, clientes por asesor.
# MAGIC   * **Meta**: cumplimiento de meta histórica (% monto vs meta).
# MAGIC * **`workspace.credito_gold.colocacion_mensual`** — colocación por agencia y mes (tendencia jun–oct 2026 para el dashboard).
# MAGIC
# MAGIC Los montos están en COP; la convención del proyecto es mostrarlos en escala $MM (millones).

# COMMAND ----------

# DBTITLE 1,Explicación métricas agencia
# MAGIC %md
# MAGIC ## 1. Métricas por agencia (`metricas_agencia`)
# MAGIC
# MAGIC Agrega todos los créditos de la silver por `codigo_oficina`. Las columnas descriptivas de la oficina (zona, municipio, asesores, metas…) son constantes por oficina, por eso se toman con `max()`. Sobre esa base se calculan los indicadores derivados: % créditos en mora, IMOR, productividad por asesor y cumplimiento de meta.

# COMMAND ----------

# DBTITLE 1,Gold: métricas por agencia (PySpark)
# CAPA GOLD: métricas de colocación, riesgo de mora y productividad por AGENCIA (PySpark)
from pyspark.sql import functions as F

silver = spark.table('workspace.credito_silver.creditos_clientes')

# 1) Agregación por oficina: los descriptivos de la oficina son constantes -> max()
base = silver.groupBy('codigo_oficina').agg(
    F.max('zona').alias('zona'),
    F.max('municipio').alias('municipio'),
    F.max('departamento').alias('departamento'),
    F.max('numero_asesores').alias('numero_asesores'),
    F.max('clientes_activos_oficina').alias('clientes_activos'),
    F.max('clientes_nuevos_periodo').alias('clientes_nuevos'),
    F.max('penetracion_mercado_pct').alias('penetracion_mercado_pct'),
    F.max('participacion_oficina_pct').alias('participacion_mercado_pct'),
    F.max('desembolso_acumulado_oficina').alias('desembolso_acumulado'),
    F.max('meta_historica_oficina').alias('meta_historica'),
    F.max('cumplimiento_meta_pct').alias('cumplimiento_meta_pct'),
    F.count('*').alias('creditos_colocados'),
    F.sum('monto_desembolsado').alias('monto_colocado'),
    F.avg('monto_desembolsado').alias('ticket_promedio_colocacion'),
    F.sum('saldo_capital').alias('saldo_capital_total'),
    F.sum(F.when(F.col('flag_en_mora') == 1, 1).otherwise(0)).alias('creditos_en_mora'),
    F.sum(F.when(F.col('flag_en_mora') == 1, F.col('saldo_capital'))).alias('saldo_capital_en_mora'),
    F.avg(F.when(F.col('flag_en_mora') == 1, F.col('dias_mora'))).alias('dias_mora_promedio'),
    F.max('dias_mora').alias('dias_mora_maximo'),
    F.avg('score_crediticio').alias('score_promedio'),
    F.sum(F.when(F.col('antiguedad_cliente_anios') == 0, 1).otherwise(0)).alias('clientes_nuevos_por_credito'),
)

# 2) Indicadores derivados: % mora, IMOR, productividad y cumplimiento de meta
metricas = base.select(
    '*',
    # RIESGO DE MORA
    F.round(100 * F.col('creditos_en_mora') / F.col('creditos_colocados'), 2).alias('pct_creditos_en_mora'),
    F.round(100 * F.col('saldo_capital_en_mora') / F.col('saldo_capital_total'), 2).alias('imor_pct'),
    # PRODUCTIVIDAD COMERCIAL
    F.round(F.col('monto_colocado') / F.col('numero_asesores') / 1e6, 2).alias('colocacion_por_asesor_MM'),
    F.round(F.col('clientes_activos') / F.col('numero_asesores'), 1).alias('clientes_por_asesor'),
    F.round(F.col('monto_colocado') / F.col('creditos_colocados'), 0).alias('ticket_promedio'),
    # COLOCACIÓN vs META
    F.round(100 * F.col('monto_colocado') / F.col('meta_historica'), 2).alias('pct_monto_vs_meta'),
)

# 3) Escribir la tabla gold
metricas.write.mode('overwrite').saveAsTable('workspace.credito_gold.metricas_agencia')

# 4) Validación: una fila por agencia, ordenada por colocación
display(spark.table('workspace.credito_gold.metricas_agencia').orderBy(F.desc('monto_colocado')))

# COMMAND ----------

# DBTITLE 1,Explicación colocación mensual
# MAGIC %md
# MAGIC ## 2. Colocación mensual (`colocacion_mensual`)
# MAGIC
# MAGIC Serie de tiempo para el gráfico de tendencia del dashboard: créditos y monto colocado por agencia y mes de desembolso.

# COMMAND ----------

# DBTITLE 1,Gold: colocación mensual (PySpark)
# CAPA GOLD: colocación mensual - tendencia para el dashboard (PySpark)
from pyspark.sql import functions as F

silver = spark.table('workspace.credito_silver.creditos_clientes')

colocacion_mensual = (
    silver.groupBy(
        F.date_trunc('month', F.col('fecha_desembolso')).alias('mes'),
        'codigo_oficina')
    .agg(
        F.max('municipio').alias('municipio'),
        F.count('*').alias('creditos_colocados'),
        F.sum('monto_desembolsado').alias('monto_colocado'),
        F.round(F.avg('monto_desembolsado'), 0).alias('ticket_promedio'),
        F.sum(F.when(F.col('flag_en_mora') == 1, 1).otherwise(0)).alias('creditos_en_mora'),
    )
    .orderBy('mes', 'codigo_oficina')
)

colocacion_mensual.write.mode('overwrite').saveAsTable('workspace.credito_gold.colocacion_mensual')

# Validación
display(spark.table('workspace.credito_gold.colocacion_mensual').orderBy('mes', F.desc('monto_colocado')))