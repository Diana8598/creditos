# CAPA SILVER: creditos_clientes - limpieza, tipado, estandarización y enriquecimiento
# Fuente: tabla bronze workspace.credito_bronze.creditos_raw (2.000 créditos crudos,
# poblada por el notebook 00 porque el archivo original BD Creditos.txt expiró).
# Lógica idéntica a la validada en el notebook "01 Silver - Limpieza y Tipado".

from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

FUENTE_BRONZE = 'workspace.credito_bronze.creditos_raw'


def _numero_es(nombre_col, tipo='decimal(18,2)'):
    """Convierte texto con formato numérico español (1.234,56) a decimal:
    quita el punto de miles y cambia la coma decimal por punto."""
    limpio = F.regexp_replace(F.trim(F.col(nombre_col).cast('string')), '[.]', '')  # quita miles
    limpio = F.regexp_replace(limpio, ',', '.')                                    # coma -> punto
    return limpio.cast(tipo)


@dp.materialized_view(
    name='workspace.credito_silver.creditos_clientes',
    comment=(
        'Créditos limpios y tipados: deduplicación por id_credito (mayor pagaré), '
        'números en formato español, fecha_desembolso desde número de serie Excel '
        '(base 1899-12-30), trim de textos, rango_mora y flag_en_mora.'
    ),
)
def creditos_clientes():
    bronce = spark.read.table(FUENTE_BRONZE)

    # DEDUPLICACIÓN: un único registro por id_credito (el de mayor pagaré)
    ventana = Window.partitionBy('id_credito').orderBy(F.col('pagare').desc())
    bronce_dedup = (
        bronce.withColumn('_rn', F.row_number().over(ventana))
        .filter(F.col('_rn') == 1)
        .drop('_rn')
    )

    return bronce_dedup.select(
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
        _numero_es('ingreso_mensual').alias('ingreso_mensual'),
        'antiguedad_cliente_anios',
        _numero_es('monto_desembolsado').alias('monto_desembolsado'),
        _numero_es('saldo_capital').alias('saldo_capital'),
        'plazo_meses',
        _numero_es('tasa_interes_ea_pct', 'decimal(5,2)').alias('tasa_interes_ea_pct'),
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
        _numero_es('penetracion_mercado_pct', 'decimal(10,4)').alias('penetracion_mercado_pct'),
        _numero_es('participacion_oficina_pct', 'decimal(10,4)').alias('participacion_oficina_pct'),
        _numero_es('ticket_promedio_oficina').alias('ticket_promedio_oficina'),
        _numero_es('desembolso_acumulado_oficina').alias('desembolso_acumulado_oficina'),
        _numero_es('meta_historica_oficina').alias('meta_historica_oficina'),
        _numero_es('cumplimiento_meta_pct', 'decimal(10,2)').alias('cumplimiento_meta_pct'),
        _numero_es('meta_sugerida_oficina').alias('meta_sugerida_oficina'),
        F.current_timestamp().alias('_processed_at'),
    )
