# Proyecto Final: Big Data — Colocación de Créditos

| | |
|---|---|
| **Módulo** | BIG DATA |
| **Docente** | Yeis Livis Taborda Henao |
| **Periodo académico** | II - 2026 |
| **Fecha de suscripción del acuerdo** | 17/04/2026 |
| **Estudiantes** | Diana Suárez · Dulfay Murillo — Universidad Autónoma Latinoamericana (UNAULA) |
| **Dominio del caso** | Colocación y riesgo de cartera de créditos de consumo, libranza, comercial y microcrédito en 8 agencias comerciales de Antioquia (Medellín: Colombia, Belén, Primero de Mayo, Alpujarra, La 80, San Javier; y Envigado e Itagüí) |

Este README resuelve, numeral por numeral, el enunciado del proyecto final aplicado al caso de negocio de **colocación de créditos**, con los resultados reales obtenidos en el EDA, el pipeline medallion, el modelo de ciencia de datos y la capa de visualización/serving construidos en Databricks.

**Resumen ejecutivo del resultado:** se procesaron **2.000 créditos de 1.900 clientes** (jun–oct 2026) por **$55.800,7 millones COP**, con una mora de **23,5% (470 créditos, saldo en mora $8.365,6M)**. Se construyó un pipeline medallion (bronce → plata → oro) sobre Unity Catalog, un modelo de riesgo de mora en producción (endpoint de serving + app de scoring) y tableros de colocación, mora y productividad por agencia.

## Tabla de contenido

1. [Caso de Negocio](#1-caso-de-negocio)
2. [Relación Beneficio/Costo](#2-relación-beneficiocosto)
3. [Arquitectura Propuesta](#3-arquitectura-propuesta)
4. [Pipeline de Ingesta de Datos](#4-pipeline-de-ingesta-de-datos)
5. [Modelos de Ciencia de Datos](#5-modelos-de-ciencia-de-datos)
6. [App o Visualization](#6-app-o-visualization)
7. [Continuous Integration (CI/CD)](#7-continuous-integration-cicd)
8. [Anexo: activos del proyecto (Databricks)](#anexo-activos-del-proyecto-databricks)
   * [Estructura del repositorio](#estructura-del-repositorio)
   * [Diagrama de arquitectura (HTML)](#diagrama-de-arquitectura-html)

---

## 1. Caso de Negocio 

### 1.1 Descripción del problema
La entidad financiera del caso desembolsa créditos en 8 agencias del área metropolitana del Valle de Aburrá sin una visión unificada de colocación, riesgo y productividad:

* La información de originación vive en hojas de cálculo dispersas (Excel con formato numérico español: coma decimal, punto de miles, fechas como número de serie), sin tipado ni control de calidad.
* No existen indicadores homogéneos de colocación ni de mora por agencia, oficina, tipo de crédito o asesor.
* La cartera presenta un nivel de mora del **23,5%** (470 de 2.000 créditos; 23,5% de los créditos y 24,1% del saldo), con buckets 1–30, 31–60 y 61–90 días.
* La decisión de aprobar un crédito no cuenta con un modelo de riesgo: el score crediticio se consulta pero no se integra a una política de alerta temprana.

### 1.2 Objetivos del proyecto

1. **Ingestar y depurar** la base de créditos en una arquitectura medallion gobernada por Unity Catalog (bronce/plata/oro).
2. **Estandarizar métricas de negocio**: colocación (créditos, monto, ticket promedio), riesgo (mora por rangos, IMOR, días promedio y máximo) y productividad (colocación y clientes por asesor).
3. **Automatizar** el refresco de las tablas con un pipeline programado (actualización diaria).
4. **Modelar el riesgo de mora** en originación para priorizar solicitudes y habilitar alertas tempranas.
5. **Democratizar el consumo** de los resultados mediante tableros (Databricks / Power BI) y una app de scoring.

### 1.3 Desafío
Convertir un archivo operativo (Excel/TSV con datos sucios) en una **plataforma de datos confiable, automatizada y gobernada**, que soporte decisiones diarias de colocación en 8 agencias, con calidad de datos verificada (0 nulos en montos, deduplicación por crédito) y trazabilidad de extremo a extremo.

### 1.4 Impacto esperado

* **Comercial:** focalizar la colocación en los segmentos con mejor relación monto/riesgo (ver numeral 5).
* **Riesgo:** anticipar la entrada en mora y priorizar la gestión de cobro sobre los $8.365,6M de saldo en mora.
* **Operativo:** una sola fuente de verdad (gold) para dirección comercial, riesgo y cada agencia.

**Entregables:** notebook `00 Bronce - Ingesta Créditos`, tablas `workspace.credito_bronze.creditos_raw` y `workspace.credito_silver.creditos_clientes`.

---

## 2. Relación Beneficio/Costo 

### 2.1 Análisis económico

**Magnitud de la cartera (jun–oct 2026, EDA sobre la capa plata):**

| Indicador | Valor |
|---|---|
| Colocación total (desembolsos) | **$55.800,7M COP** |
| Créditos / clientes | 2.000 / 1.900 |
| Ticket promedio | $27,9M (rango $2M – $80M) |
| Saldo capital total | $34.692,1M |
| Saldo en mora (470 créditos) | **$8.365,6M** (24,1% del saldo) |
| Tasa EA promedio por producto | 16,5% (Libranza) – 22,0% (Microcrédito) |

Cada punto porcentual de mora que se recupere o prevenga representa ≈ **$346,9M de saldo** (1% de la cartera) que sigue generando interés en lugar de convertirse en pérdida. Los 120 créditos con mora > 30 días ($1.961,2M de saldo) son el segmento de mayor costo: a mayor días de mora, menor score promedio (681 → 639 → 582), señal de deterioro progresivo.

### 2.2 Retorno de inversión (escenario ilustrativo, supuestos explícitos)

* **Beneficio:** el modelo de riesgo (numeral 5) funciona como alerta temprana. Si con AUC 0,645 se interviniera preventivamente el 20% de los créditos que entrarían en mora y se evitara la pérdida equivalente al 10% de su saldo promedio en mora ($17,8M/crédito): 94 créditos × $17,8M × 10% ≈ **$167M evitados por ciclo**. El efecto se multiplica si la intervención se aplica a la cartera vigente, no solo a originaciones nuevas.
* **Costo:** la plataforma corre en Databricks serverless (cómputo bajo demanda, sin clústeres permanentes); la ingesta reutiliza el archivo ya disponible y el refresco diario es una única actualización de pipeline. El costo marginal dominante es el tiempo de desarrollo, ya ejecutado.
* **ROI = (Beneficio − Costo) / Costo.** Con un costo anual de plataforma conservador frente a cientos de millones de saldo en riesgo gestionable, la relación beneficio/costo es **altamente favorable**: el modelo se justifica con que apenas prevenga una fracción de un punto de la mora.

### 2.3 Mayores ingresos
El EDA identifica dónde está el margen:

* **Consumo** es el motor de volumen (780 créditos, $18.234,9M) pero con ticket bajo ($23,4M).
* **Comercial** tiene el ticket más alto ($40,4M) y **Libranza** la mejor combinación monto/riesgo por descuento de nómina ($17.964,7M en 544 créditos, EA 16,5%).
* **Microcrédito** tiene el ticket más bajo ($14,3M) y la tasa más alta (22%), pero es el más pequeño en monto ($4.222,5M): conviene evaluar si su retorno ajustado por riesgo justifica la capacidad operativa.
* Geográficamente, **Envigado** lidera la colocación ($11.020M) pero **San Javier** tiene la mora más alta (26,6%) con el menor monto ($2.995,2M): reasignar capacidad comercial hacia zonas de mejor calidad de cartera aumenta el ingreso esperado sin crecer en riesgo.

**Entregables:** este numeral se soporta en `03 EDA y Validación Créditos` y en las tablas gold `metricas_agencia` / `colocacion_mensual`.

---

## 3. Arquitectura Propuesta 

### 3.1 Diagrama de arquitectura

   <img width="650" height="472" alt="image" src="https://github.com/user-attachments/assets/e130f963-96d4-45b9-9685-8cd5550d920b" />



> Versión visual del diagrama (capas medallion + ML + consumo): [`docs/diagrama-arquitectura-medallion.html`](docs/diagrama-arquitectura-medallion.html)

### 3.2 Explicación técnica

* **Formato y gobierno:** todas las capas son tablas **Delta en Unity Catalog** (catálogo `workspace`, esquemas `credito_bronze`, `credito_silver`, `credito_gold` y `credito_ml`), con linaje automático, control de acceso y comentarios de metadatos.
* **Ingesta:** notebook de bronce que carga el archivo crudo sin transformar, con protección ante expiración de la ruta temporal (si el archivo no está disponible, conserva la tabla existente en lugar de truncar).
* **Transformaciones:** la capa plata materializa reglas de limpieza; el oro materializa agregaciones de negocio como **vistas materializadas** de un pipeline, por lo que se recalculan de forma consistente en cada actualización.
* **Consumo:** los tableros y la app leen de gold (nunca de bronce), garantizando una única definición de cada métrica.
* **Serving:** el modelo se versiona en el Model Registry y se expone vía endpoint gestionado; la app consume el endpoint con autenticación delegada (OBO).
* **Automatización:** el pipeline se ejecuta mediante un job programado diariamente (ver numeral 4).

---

## 4. Pipeline de Ingesta de Datos 

### 4.1 Estrategia medallion

| Capa | Tabla | Rol | Reglas clave |
|---|---|---|---|
| **Bronce** | `workspace.credito_bronze.creditos_raw` | Réplica cruda de la fuente | Sin transformaciones de negocio; renombrado únicamente de columnas con espacios (no admitidas por Delta) |
| **Plata** | `workspace.credito_silver.creditos_clientes` | Créditos limpios y tipados | Ver 4.2 |
| **Oro** | `workspace.credito_gold.metricas_agencia` | KPIs por agencia | Colocación, IMOR, productividad por asesor, cumplimiento de meta |
| **Oro** | `workspace.credito_gold.colocacion_mensual` | Serie temporal | Colocación por agencia/oficina y mes de desembolso |

### 4.2 Automatización de la ingesta y elaboración de la limpieza
La limpieza se implementa en PySpark (DataFrame API) y quedó automatizada en las transformaciones del pipeline:

1. **Números en formato español:** `regexp_replace(trim(CAST(col AS STRING)), '[.]', '')` seguido de `replace(',', '.')` para miles y decimales (p.ej. `1.250.000,50` → `1250000.50`), y cast a `DOUBLE`/`DECIMAL`.
2. **Fechas:** conversión del número de serie de Excel con base `1899-12-30` (`date_add(lit('1899-12-30'), serial)`) → `fecha_desembolso` tipo `DATE`.
3. **Deduplicación por crédito:** se conserva el registro con **mayor pagaré** por `id_credito` mediante `Window.partitionBy('id_credito').orderBy(desc('pagare'))` + `row_number() == 1`.
4. **Textos:** `trim` de todas las columnas categóricas (agencia, oficina, tipo de crédito, estado).
5. **Derivadas de riesgo:** `dias_mora` → `rango_mora` (`Sin mora` / `1-30 dias` / `31-60 dias` / `61-90 dias` con `when/between`) y bandera `flag_en_mora`.
6. **Priorización:** la ingesta reutiliza la tabla bronce existente; si la fuente temporal expira, el pipeline no destruye datos (protección anti-truncado).

### 4.3 Pipeline (Lakeflow Spark Declarative Pipelines) y programación

* Pipeline **“Pipeline Créditos Medallion”**: 3 vistas materializadas (`creditos_clientes`, `metricas_agencia`, `colocacion_mensual`) definidas en Python (`@dp.materialized_view`) bajo `transformations/{silver,gold}/*.py`, con fuente = tabla bronce existente.
* El gold **no se calcula aparte**: se deriva del silver dentro del mismo pipeline, garantizando consistencia de métricas.
* **Actualización real ejecutada y validada:** las tablas silver/gold son gestionadas por el pipeline tras eliminar las creadas manualmente por los notebooks (ver 4.4).
* **Job programado:** *Actualización diaria - Pipeline Créditos Medallion* refresca la plata y el oro cada día.

### 4.4 Validación de calidad de datos (resultados del EDA)

| Chequeo | Resultado | Estado |
|---|---|---|
| Filas en plata | 2.000 | ✔ |
| Créditos únicos (`id_credito`) | 2.000 (sin duplicados tras dedup por pagaré) | ✔ |
| Clientes únicos (`cedula`) | 1.900 (110 clientes con 2+ créditos) | ✔ |
| Nulos en montos, saldo, días de mora y score | 0 en todas | ✔ |
| Rango `monto_desembolsado` | $2M – $80M (prom $27,9M) | ✔ |
| Rango `ingreso_mensual` | $1,61M – $11,12M (prom $4,28M) | ✔ |
| Rango `score_crediticio` | 445 – 950 (prom 696) | ✔ |
| Rango `dias_mora` | 0 – 90 (prom 8,1) | ✔ |
| Cobertura geográfica | 8 oficinas / 8 zonas | ✔ |
| Ventana temporal | 2026-06-01 a 2026-10-31 | ✔ |

Cruce de consistencia plata ↔ oro: la suma de `creditos_colocados` en gold (2.000) y de `creditos_en_mora` (470) coincide exactamente con la plata; los 40 registros mensuales corresponden a 5 meses × 8 agencias.

**Entregables:** notebooks `00 Bronce`, `01 Silver`, `02 Gold`, pipeline SDP y job diario.

---

## 5. Modelos de Ciencia de Datos 

### 5.1 Estadística descriptiva (EDA)

## Arquitectura del Pipeline

 <img width="1050" height="365" alt="image" src="https://github.com/user-attachments/assets/7e311ea6-4aa2-4fed-9137-9f26577608cf" />


**Colocación por tipo de crédito:**

| Tipo | Créditos | Monto ($M) | Ticket prom. ($M) | Tasa EA |
|---|---:|---:|---:|---:|
| Consumo | 780 | 18.234,9 | 23,38 | 18,5% |
| Libranza | 544 | 17.964,7 | 33,02 | 16,5% |
| Comercial | 381 | 15.378,6 | 40,36 | 20,0% |
| Microcredito | 295 | 4.222,5 | 14,31 | 22,0% |

**Riesgo de mora por rango** (470 créditos en mora, 23,5%):

| Rango | Créditos | Saldo ($M) | Días prom. | Score prom. |
|---|---:|---:|---:|---:|
| Sin mora | 1.530 | 26.326,5 | 0 | 705 |
| 1-30 dias | 350 | 6.404,4 | 23 | 681 |
| 31-60 dias | 89 | 1.571,9 | 60 | 639 |
| 61-90 dias | 31 | 389,3 | 90 | 582 |

**Colocación por agencia** (orden por monto):

| Agencia (zona) | Créditos | Monto ($M) | % mora |
|---|---:|---:|---:|
| Envigado | 269 | 11.020,0 | 24,9% |
| Medellín — Alpujarra | 273 | 8.193,5 | 20,9% |
| Medellín — Belén | 240 | 7.651,4 | 21,3% |
| Medellín — La 80 | 262 | 7.548,7 | 24,0% |
| Medellín — Primero de Mayo | 278 | 7.238,9 | 24,5% |
| Medellín — Colombia | 240 | 5.998,2 | 21,3% |
| Itagüí | 246 | 5.154,8 | 25,2% |
| Medellín — San Javier | 192 | 2.995,2 | 26,6% |

**Perfil del cliente:** 51,2% mujeres (1.023) vs 48,8% hombres (977); solteros son el grupo más numeroso en ambos géneros (407 F / 377 M); edad promedio 40,2 años (20–60); ingreso promedio $4,28M; la penetración y participación vienen expresadas en puntos porcentuales.

### 5.2 Análisis estadístico

* **Score crediticio vs mora:** gradiente monotónico claro — a mayor días de mora, menor score promedio (705 al día → 582 con 61–90 días). El score de originación **sí contiene señal predictiva** de deterioro.
* **Concentración de riesgo por zona:** San Javier (26,6%), Itagüí (25,2%) y Envigado (24,9%) superan el promedio (23,5%); Alpujarra es la mejor (20,9%). La diferencia entre extremos es de ~5,7 puntos porcentuales.
* **Producto vs riesgo:** el microcrédito combina el ticket más bajo con la tasa más alta, señal de mayor riesgo percibido; la libranza (descuento de nómina) es el producto con mejor perfil.
* **Calidad:** 0 nulos y rangos plausibles en todas las variables clave; no hay créditos duplicados ni montos atípicos fuera de $2M–$80M.

### 5.3 Modelado (modelo de riesgo de mora)

* **Algoritmo:** Regresión Logística (binaria: crédito en mora sí/no) entrenada en `04 Modelo Riesgo Mora - Entrenamiento y Serving`, con experimento MLflow `riesgo_mora_creditos`.
* **Variables (14 features de originación, sin fuga de datos):** 6 categóricas + 8 numéricas. Se **excluyeron** `dias_mora`, `estado_credito`, `rango_mora` y `saldo_capital` por ser variables posteriores al desembolso (fuga).
* **Validación (holdout):** exactitud **0,77**, **ROC-AUC 0,645**, recall de la clase mora **0,043**. El AUC confirma capacidad discriminativa moderada superior al azar; el recall bajo refleja el desbalance de clases (23,5% positivos) y define la siguiente iteración: ajuste de umbral, ponderación de clases o re-muestreo para priorizar detección de morosos.
* **Modelo final:** re-entrenado sobre las 2.000 filas y registrado como `workspace.credito_ml.modelo_riesgo_mora` **v1** en el Model Registry (run `b07e905111dd41caa7de3c0da3ff40b0`).
* **Uso operativo:** el score del modelo prioriza solicitudes y alimenta la app de scoring (numeral 6); complementa —no reemplaza— el score crediticio del buró.

---

## 6. App o Visualization 

### 6.1 Visualization
## Orquestación con Databricks Jobs

 <img width="1034" height="784" alt="image" src="https://github.com/user-attachments/assets/7b013aa5-64ce-4756-9034-93e24f9fcae3" />

## APP "Scoring de Riesgo de Mora"
   Score crediticio permite a las entidades financieras evaluar el nivel de riesgo y facilitar a mejores condiciones de crédito.
   
  <img width="1034" height="630" alt="image" src="https://github.com/user-attachments/assets/a364069a-06e8-46eb-9e98-0952a7761fdc" />
  <img width="1291" height="847" alt="Captura de pantalla 2026-11-19 190040" src="https://github.com/user-attachments/assets/97705abe-809e-45e4-93d3-b76b65ee329a" />

   
* **Dashboard “Créditos - Colocación, Mora y Productividad por Agencia”**: KPIs de colocación, IMOR y productividad sobre las tablas gold.
* **Dashboard “Colocación de Créditos por Oficina”**: 4 KPIs (total 2.000 créditos, $55.800,7M, ticket promedio $27,4M, 470 en mora), barras por oficina, línea mensual, pivote oficina × mes (jun–oct 2026) y tabla de detalle, con montos en COP compacto.
* **Power BI:** conexión directa vía conector Databricks sobre las tablas gold (misma fuente de verdad).

  <img width="1211" height="706" alt="image" src="https://github.com/user-attachments/assets/3dcaa929-a906-4957-9f40-2114ada60ef2" />
  <img width="1228" height="278" alt="image" src="https://github.com/user-attachments/assets/4bc88d35-9630-43f7-9b63-3babc551d0ae" />
  <img width="1222" height="378" alt="image" src="https://github.com/user-attachments/assets/1a53839d-d92d-4bea-a6da-c28a798cb656" />
  <img width="1194" height="370" alt="image" src="https://github.com/user-attachments/assets/ddbf3961-9b30-4de7-9be3-352be63248b9" />




### 6.2 Serving Endpoint

* Endpoint de Model Serving **“creditos”** (estado READY) que expone `workspace.credito_ml.modelo_riesgo_mora` v1; invocación por `dataframe_records` con las 14 features.
* **App “scoring-creditos”** (Databricks App, desplegada): formulario Flask que envía los datos de una solicitud al endpoint con autenticación OBO y devuelve la probabilidad de mora, habilitando la decisión de aprobación en línea para los asesores.

---

## 7. Continuous Integration (CI/CD)

El proyecto ya satisface parcialmente los criterios y define una hoja de ruta para completarlos:

**Recomendaciones e índices de calidad (implementado):**

* Validaciones del EDA convertidas en chequeos repetibles (conteos, unicidad, nulos, rangos): la tabla 4.4 es el checklist de calidad de cada actualización del pipeline.
* Consistencia plata ↔ oro verificada (2.000 créditos y 470 en mora coinciden entre capas).
* Versionamiento del código en Git (repositorio `creditos`) y del modelo en el Model Registry (v1).

**Recomendaciones (por hacer):**

1. **Diagramas y dashboards como código:** llevar el pipeline, el job y los dashboards a un bundle (Declarative Automation Bundle) versionado, con despliegue reproducible por entorno (dev → prod).
2. **MLflow logs con métricas anexas:** registrar en cada corrida, además de las métricas del modelo, las métricas de calidad de datos (nulos, duplicados, IMOR) como trazabilidad del pipeline; añadir pruebas unitarias de las transformaciones (formato numérico español, fechas Excel, dedup) y expectativas de datos en la plata (p.ej. `monto_desembolsado` > 0).
3. **Código limpio:** convenciones consistentes (snake_case, nombres sin tildes/espacios en tablas y columnas — p.ej. el esquema es `credito_bronze`), docstrings en las transformaciones y revisión por pull request antes de promover cambios.
4. **Monitoreo:** alertas si el job diario falla o si el IMOR de gold supera un umbral (p.ej. 25%).

---

## Anexo: activos del proyecto (Databricks)

| Activo | Nombre / ubicación |
|---|---|
| Notebooks | `00 Bronce - Ingesta Créditos`, `01 Silver - Limpieza y Tipado`, `02 Gold - Métricas de Negocio`, `03 EDA y Validación Créditos`, `04 Modelo Riesgo Mora - Entrenamiento y Serving` (carpeta Drafts) |
| Pipeline | *Pipeline Créditos Medallion* (vistas materializadas silver + gold, refresco diario) |
| Tablas | `workspace.credito_bronze.creditos_raw`, `workspace.credito_silver.creditos_clientes`, `workspace.credito_gold.metricas_agencia`, `workspace.credito_gold.colocacion_mensual` |
| Modelo | `workspace.credito_ml.modelo_riesgo_mora` v1 · endpoint *creditos* · app *scoring-creditos* |
| Dashboards | *Créditos - Colocación, Mora y Productividad por Agencia* · *Colocación de Créditos por Oficina* |

### Estructura del repositorio

```
creditos/
├── README.md
├── notebooks/          # 00 bronce, 01 silver, 02 gold, 03 EDA, 04 modelo (exportados .py SOURCE)
├── pipeline/
│   ├── bronze/         # ingesta de la capa bronce (00-bronce-ingesta-creditos.py)
│   ├── silver/         # creditos_clientes.py (MV plata del pipeline SDP)
│   └── gold/           # metricas_agencia.py, colocacion_mensual.py (MV oro)
├── dashboards/         # dashboards AI/BI versionados como código
│   ├── colocacion-creditos-oficina.lvdash.json
│   └── creditos-colocacion-mora-productividad-agencia.lvdash.json
├── app/                # app.py + app.yaml (Databricks App scoring-creditos)
└── docs/               # diagrama-arquitectura-medallion.html
```

### Diagrama de arquitectura (HTML)

El diagrama completo de la arquitectura medallion —fuente, bronce, plata, oro, capa ML (modelo, registry y serving) y consumo (dashboards, app y Power BI)— está en [`docs/diagrama-arquitectura-medallion.html`](docs/diagrama-arquitectura-medallion.html). Ábrelo en cualquier navegador; no requiere dependencias externas.

### 📚 Referencias y Recursos |

### Imágenes del Proyecto

 **arquitectura_medallion.png:** Diagrama de arquitectura Medallion.
 
**pipeline_ingesta_1.png, pipeline_ingesta_2.png: Pipeline de ingesta.

**job_automatizado_1.png, job_automatizado_2.png: Configuración de jobs.

**resultados_modelos.png: Dashboard de resultados de ML.

**eda_completo.png: Visualizaciones de análisis exploratorio.

**app_Scoring de Riesgo: Capturas de la aplicación.

**dashboard_créditos_1.png, dashboard_oficinas y mora_2.png: Dashboard de métricas.

