# Databricks notebook source
# DBTITLE 1,Título: 04 Modelo Riesgo Mora
# MAGIC %md
# MAGIC # 04 Modelo Riesgo de Mora — Entrenamiento, Registro y Serving
# MAGIC
# MAGIC **Paso 04 del pipeline de créditos.** Entrena automáticamente un modelo de clasificación que predice si un crédito entrará en mora (`flag_en_mora`) a partir de características de originación (perfil del cliente, tipo de crédito, monto, plazo, tasa y score crediticio). El flujo: EDA → pipeline sklearn (imputación + escalado + one-hot + regresión logística) → métricas en holdout → refit final → registro del experimento en MLflow → validación aislada (uv) → registro del modelo en Unity Catalog (`workspace.credito_ml.modelo_riesgo_mora`) → base para el serving endpoint y la app.
# MAGIC
# MAGIC * Excluye por diseño las variables de fuga (`dias_mora`, `estado_credito`, `rango_mora`, `saldo_capital`): solo usan características disponibles al momento de originar el crédito.
# MAGIC * Datos: `workspace.credito_silver.creditos_clientes` (2.000 créditos, 23,5% en mora).

# COMMAND ----------

# DBTITLE 1,Setup: uv para validación
# uv: se usa en el paso de validación aislada del modelo (instala los requisitos del artefacto desde cero)
%pip install --quiet uv

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,EDA: target y features
# EDA: target, features y correlaciones (2.000 créditos de la silver)
import pandas as pd
import matplotlib.pyplot as plt

COLS_CAT = ['tipo_credito', 'ocupacion', 'genero', 'estado_civil', 'nivel_educativo', 'zona']
COLS_NUM = ['edad', 'ingreso_mensual', 'antiguedad_cliente_anios', 'monto_desembolsado',
            'plazo_meses', 'tasa_interes_ea_pct', 'score_crediticio', 'estrato']

df = (spark.table('workspace.credito_silver.creditos_clientes')
      .select('flag_en_mora', *COLS_CAT, *COLS_NUM)
      .toPandas())
df[COLS_NUM] = df[COLS_NUM].astype('float64')  # Decimal de Spark -> float64 (serializable en MLflow)
y = df['flag_en_mora'].astype('int64')
print(f"Filas: {len(df)} | Features: {len(COLS_CAT)} categóricas + {len(COLS_NUM)} numéricas")

display(pd.DataFrame({'feature': COLS_CAT + COLS_NUM,
                      'tipo': ['categórica'] * len(COLS_CAT) + ['numérica'] * len(COLS_NUM)}))

# Distribución del target
share = df['flag_en_mora'].value_counts(normalize=True)
print(f"Al día: {share.get(0, 0):.1%} | En mora: {share.get(1, 0):.1%}")

fig, axes = plt.subplots(1, 2, figsize=(10, 3.2))
(df['flag_en_mora'].map({0: 'Al día', 1: 'En mora'}).value_counts()
 .plot.bar(ax=axes[0], color=['#4c9f70', '#c0504d'], title='Distribución del target (mora)'))
axes[0].set_ylabel('créditos')
corr = df[COLS_NUM + ['flag_en_mora']].corr()['flag_en_mora'].drop('flag_en_mora').sort_values()
corr.plot.barh(ax=axes[1], color='#3b7dd8', title='Correlación de numéricas con mora')
axes[1].set_xlabel('correlación')
plt.tight_layout(); plt.show()

display(df[COLS_NUM].describe().T.round(1))

# COMMAND ----------

# DBTITLE 1,Entrenamiento automático + evaluación holdout
# ENTRENAMIENTO AUTOMÁTICO: pipeline sklearn completo (preprocesamiento dentro del modelo)
from sklearn.base import clone
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (accuracy_score, precision_score, recall_score, f1_score,
                             roc_auc_score, log_loss, confusion_matrix)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

X = df[COLS_CAT + COLS_NUM]
y = df['flag_en_mora']

# Créditos intercambiables (predicción en originación): split estratificado aleatorio, holdout 25%
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, stratify=y, random_state=42)
print(f"Train: {len(X_train)} | Test: {len(X_test)} | Mora en train: {y_train.mean():.1%}")

prepro = ColumnTransformer([
    ('num', Pipeline([('imp', SimpleImputer(strategy='median')), ('sc', StandardScaler())]), COLS_NUM),
    ('cat', Pipeline([('imp', SimpleImputer(strategy='most_frequent')),
                      ('oh', OneHotEncoder(handle_unknown='ignore'))]), COLS_CAT),
])

modelo = Pipeline([('pre', prepro),
                   ('clf', LogisticRegression(max_iter=1000, random_state=42))])
modelo.fit(X_train, y_train)

y_pred = modelo.predict(X_test)
y_prob = modelo.predict_proba(X_test)[:, 1]

holdout = {
    'accuracy': accuracy_score(y_test, y_pred),
    'precision': precision_score(y_test, y_pred),
    'recall': recall_score(y_test, y_pred),
    'f1': f1_score(y_test, y_pred),
    'roc_auc': roc_auc_score(y_test, y_prob),
    'log_loss': log_loss(y_test, y_prob),
}
print({k: round(v, 3) for k, v in holdout.items()})

display(pd.DataFrame(confusion_matrix(y_test, y_pred),
                     index=['real al día', 'real en mora'],
                     columns=['pred al día', 'pred en mora']))

# COMMAND ----------

# DBTITLE 1,MLflow: refit final y log del modelo
# Refit final con TODOS los datos + registro del experimento en MLflow
import mlflow
from mlflow.models import infer_signature

# Mismas features e hiperparámetros; las métricas reportadas son del holdout anterior
modelo_final = clone(modelo).fit(X, y)

mlflow.set_experiment('/Users/diana.suarez4649@unaula.edu.co/riesgo_mora_creditos')
signature = infer_signature(X.head(100), modelo_final.predict(X.head(100)))

with mlflow.start_run(run_name='logreg_riesgo_mora_v1') as run:
    mlflow.log_params({'modelo': 'LogisticRegression', 'max_iter': 1000,
                       'features': len(COLS_CAT) + len(COLS_NUM),
                       'n_train': len(X_train), 'n_test': len(X_test), 'n_final_fit': len(X)})
    mlflow.log_metrics({f'holdout_{k}': float(v) for k, v in holdout.items()})
    model_info = mlflow.sklearn.log_model(modelo_final, name='modelo_riesgo_mora',
                                          signature=signature, input_example=X.head(3))
    run_id = run.info.run_id

print(f"Run: {run_id}\nModel URI: {model_info.model_uri}")

# COMMAND ----------

# DBTITLE 1,Validación aislada y registro en UC
# VALIDACIÓN AISLADA (ambiente limpio con uv) + REGISTRO EN UNITY CATALOG
import mlflow

pyfunc = mlflow.pyfunc.load_model(model_info.model_uri)
assert pyfunc.metadata.signature is not None, 'El modelo no tiene firma'
assert pyfunc.input_example is not None, 'El modelo no tiene ejemplo de entrada'

# Instala los pip_requirements del artefacto desde cero y predice: prueba que el modelo sirve
resultado = mlflow.models.predict(model_uri=model_info.model_uri,
                                  input_data=pyfunc.input_example,
                                  env_manager='uv')
print('Validación aislada OK:'); display(resultado)

# Esquema ML + registro del modelo en Unity Catalog
spark.sql('CREATE SCHEMA IF NOT EXISTS workspace.credito_ml')
mlflow.set_registry_uri('databricks-uc')
NOMBRE_UC = 'workspace.credito_ml.modelo_riesgo_mora'
version = mlflow.register_model(model_uri=model_info.model_uri, name=NOMBRE_UC,
                                await_registration_for=300)
print(f"Modelo registrado: {NOMBRE_UC} versión {version.version}")
print(f"URI para serving: models:/{NOMBRE_UC}/{version.version}")