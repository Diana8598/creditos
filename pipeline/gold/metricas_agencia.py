# CAPA GOLD: metricas_agencia - una fila por agencia con colocación, riesgo de mora,
# productividad comercial y cumplimiento de meta histórica.
# Lógica idéntica a la validada en el notebook "02 Gold - Métricas de Negocio".

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER = 'workspace.credito_silver.creditos_clientes'


@dp.materialized_view(
    name='workspace.credito_gold.metricas_agencia',
    comment=(
        'Métricas por agencia: colocación (créditos, monto, ticket), riesgo de mora '
        '(% créditos en mora, IMOR, días promedio y máximo), productividad '
        '(colocación y clientes por asesor) y cumplimiento de meta histórica.'
    ),
)
def metricas_agencia():
    silver = spark.read.table(SILVER)

    # Descriptivos de la oficina son constantes -> max(); métricas de crédito -> sum/avg/count
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

    # Indicadores derivados
    return base.select(
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
