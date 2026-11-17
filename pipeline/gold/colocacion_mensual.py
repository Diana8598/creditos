# CAPA GOLD: colocacion_mensual - colocación por agencia y mes de desembolso
# (tendencia jun-oct 2026 para el dashboard).
# Lógica idéntica a la validada en el notebook "02 Gold - Métricas de Negocio".

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER = 'workspace.credito_silver.creditos_clientes'


@dp.materialized_view(
    name='workspace.credito_gold.colocacion_mensual',
    comment='Colocación por agencia y mes de desembolso: créditos, monto, ticket promedio y créditos en mora.',
)
def colocacion_mensual():
    silver = spark.read.table(SILVER)

    return (
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
    )
