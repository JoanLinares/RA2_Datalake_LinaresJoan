#!/usr/bin/env python3
"""
Spark Cleaner - Limpieza de datos con PySpark antes de cargar a NeonDB
=========================================================================
Recibe DataFrames de pandas, los limpia con Spark y los devuelve en pandas.

Limpieza aplicada:
  - Elimina filas con ID nulo o vacío (requisito de PK)
  - Elimina duplicados por ID (conserva el más reciente por updatedAt)
  - Rellena nulos en columnas numéricas con 0
  - Rellena nulos en columnas de texto con 'N/A'
  - Elimina filas con pregunta/título vacío en tablas críticas
  - Trunca strings largos para respetar límites de columna
"""
import logging
import os
import sys

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

# ── Apuntar PySpark al Python del venv (evita el alias de Microsoft Store) ──
_python_exe = sys.executable
os.environ["PYSPARK_PYTHON"] = _python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = _python_exe


def _get_spark() -> SparkSession:
    """Crea (o reutiliza) una SparkSession local ligera."""
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("RA2_Warehouse_Cleaner")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", _python_exe)
        .config("spark.pyspark.driver.python", _python_exe)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )


class SparkCleaner:
    """Limpieza de DataFrames con PySpark puro."""

    # ── Helpers internos ──────────────────────────────────────────────
    @staticmethod
    def _normalize_nulls(sdf, columns):
        """
        Convierte strings 'None', 'null', 'N/A', '' y espacios en blanco
        a NULL real de Spark, para que fillna() pueda actuar después.
        """
        for c in columns:
            if c in sdf.columns:
                sdf = sdf.withColumn(
                    c,
                    F.when(
                        F.trim(F.col(c).cast(StringType())).isin(
                            "", "None", "none", "null", "NULL", "N/A", "n/a", "NA", "NaN"
                        ),
                        F.lit(None),
                    ).otherwise(F.col(c)),
                )
        return sdf

    # ── Mercados ──────────────────────────────────────────────────────
    @staticmethod
    def clean_markets(df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia el DataFrame de mercados gaming.

        Reglas:
        - Normaliza strings tipo 'None'/'null'/'' a NULL real
        - Elimina filas sin 'id' o sin 'question'
        - Deduplica por 'id', conservando la fila con 'updatedAt' más reciente
        - Rellena NaN numéricos con 0.0
        - Rellena resolutionSource null con 'Sin fuente'
        - Rellena slug null con id
        - Rellena outcomes null con '[]'
        - Trunca strings largos a 500 chars
        """
        if df.empty:
            return df

        logger.info("  [SparkCleaner] Limpiando mercados...")
        initial = len(df)

        spark = _get_spark()
        sdf = spark.createDataFrame(df)

        # 0. Normalizar strings falsos a NULL real
        text_cols = [c for c, t in sdf.dtypes if t == "string"]
        sdf = SparkCleaner._normalize_nulls(sdf, text_cols)

        # 1. Eliminar filas sin id o sin question
        sdf = sdf.filter(
            F.col("id").isNotNull() & (F.trim(F.col("id").cast(StringType())) != "") &
            F.col("question").isNotNull() & (F.trim(F.col("question").cast(StringType())) != "")
        )

        # 2. Deduplicar por id (más reciente por updatedAt)
        sort_col = "updatedAt" if "updatedAt" in sdf.columns else ("createdAt" if "createdAt" in sdf.columns else None)
        if sort_col:
            window = Window.partitionBy("id").orderBy(F.col(sort_col).desc())
            sdf = sdf.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
        else:
            sdf = sdf.dropDuplicates(["id"])

        # 3. Rellenar nulos numéricos con 0
        numeric_cols = [
            "volume", "volume24hr", "volume1wk", "volume1mo", "volume1yr",
            "liquidity", "liquidityAmm", "liquidityClob",
            "lastTradePrice", "bestBid", "bestAsk", "spread", "openInterest",
        ]
        fill_numeric = {c: 0.0 for c in numeric_cols if c in sdf.columns}
        if fill_numeric:
            sdf = sdf.fillna(fill_numeric)

        # 4. Rellenar nulos en strings críticos
        str_defaults = {
            "resolutionSource": "Sin fuente",
            "slug": "sin-slug",
            "outcomes": "[]",
            "description": "Sin descripción",
        }
        fill_str = {c: v for c, v in str_defaults.items() if c in sdf.columns}
        if fill_str:
            sdf = sdf.fillna(fill_str)

        # 5. Si slug sigue vacío, usar el id
        if "slug" in sdf.columns:
            sdf = sdf.withColumn(
                "slug",
                F.when(F.col("slug") == "sin-slug", F.col("id").cast(StringType())).otherwise(F.col("slug")),
            )

        # 6. Asegurar booleanos (active/closed) — no dejar nulls
        for bool_col in ["active", "closed"]:
            if bool_col in sdf.columns:
                sdf = sdf.withColumn(bool_col, F.coalesce(F.col(bool_col).cast("boolean"), F.lit(False)))

        # 7. Truncar strings críticos
        for col_name, max_len in {"question": 500, "slug": 500, "resolutionSource": 500}.items():
            if col_name in sdf.columns:
                sdf = sdf.withColumn(col_name, F.substring(F.col(col_name).cast(StringType()), 1, max_len))

        result = sdf.toPandas()
        cleaned = initial - len(result)
        logger.info(f"  [SparkCleaner] Mercados: {initial:,} → {len(result):,} (eliminados {cleaned:,})")
        logger.info(f"  [SparkCleaner] Nulls corregidos → resolutionSource, slug, outcomes, booleanos")
        return result

    # ── Eventos ───────────────────────────────────────────────────────
    @staticmethod
    def clean_events(df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia el DataFrame de eventos gaming.

        Reglas:
        - Normaliza strings tipo 'None'/'null'/'' a NULL real
        - Elimina filas sin 'id'
        - Deduplica por 'id' (más reciente por updatedAt)
        - Rellena category/subcategory null con 'Gaming'/'General'
        - Rellena title null con ticker o slug
        - Rellena resolutionSource null con 'Sin fuente'
        - Asegura que booleanos no son null
        """
        if df.empty:
            return df

        logger.info("  [SparkCleaner] Limpiando eventos...")
        initial = len(df)

        spark = _get_spark()
        sdf = spark.createDataFrame(df)

        # 0. Normalizar strings falsos a NULL real
        text_cols = [c for c, t in sdf.dtypes if t == "string"]
        sdf = SparkCleaner._normalize_nulls(sdf, text_cols)

        # 1. Eliminar filas sin id
        sdf = sdf.filter(
            F.col("id").isNotNull() & (F.trim(F.col("id").cast(StringType())) != "")
        )

        # 2. Deduplicar por id
        if "updatedAt" in sdf.columns:
            window = Window.partitionBy("id").orderBy(F.col("updatedAt").desc())
            sdf = sdf.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
        else:
            sdf = sdf.dropDuplicates(["id"])

        # 3. Rellenar category y subcategory con valores útiles (no 'N/A')
        if "category" in sdf.columns:
            sdf = sdf.withColumn("category", F.coalesce(F.col("category"), F.lit("Gaming")))
        if "subcategory" in sdf.columns:
            sdf = sdf.withColumn("subcategory", F.coalesce(F.col("subcategory"), F.lit("General")))

        # 4. Rellenar title: si null, usar ticker o slug o 'Sin título'
        if "title" in sdf.columns:
            fallback = F.lit("Sin título")
            if "ticker" in sdf.columns:
                fallback = F.coalesce(F.col("ticker"), fallback)
            if "slug" in sdf.columns:
                fallback = F.coalesce(F.col("slug"), fallback)
            sdf = sdf.withColumn("title", F.coalesce(F.col("title"), fallback))

        # 5. Rellenar otros strings nulos
        str_defaults = {
            "resolutionSource": "Sin fuente",
            "ticker": "sin-ticker",
            "slug": "sin-slug",
        }
        fill_str = {c: v for c, v in str_defaults.items() if c in sdf.columns}
        if fill_str:
            sdf = sdf.fillna(fill_str)

        # 6. Asegurar booleanos no null
        for bool_col in ["active", "closed", "featured"]:
            if bool_col in sdf.columns:
                sdf = sdf.withColumn(bool_col, F.coalesce(F.col(bool_col).cast("boolean"), F.lit(False)))

        result = sdf.toPandas()
        cleaned = initial - len(result)
        logger.info(f"  [SparkCleaner] Eventos: {initial:,} → {len(result):,} (eliminados {cleaned:,})")
        logger.info(f"  [SparkCleaner] Nulls corregidos → category='Gaming', subcategory='General', resolutionSource, title")
        return result

    # ── Series ────────────────────────────────────────────────────────
    @staticmethod
    def clean_series(df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia el DataFrame de series gaming.

        Reglas:
        - Normaliza strings tipo 'None'/'null'/'' a NULL real
        - Elimina filas sin 'id'
        - Deduplica por 'id'
        - Rellena title/slug/description nulos
        """
        if df.empty:
            return df

        logger.info("  [SparkCleaner] Limpiando series...")
        initial = len(df)

        spark = _get_spark()
        sdf = spark.createDataFrame(df)

        # 0. Normalizar strings falsos a NULL real
        text_cols = [c for c, t in sdf.dtypes if t == "string"]
        sdf = SparkCleaner._normalize_nulls(sdf, text_cols)

        # 1. Eliminar sin id
        sdf = sdf.filter(
            F.col("id").isNotNull() & (F.trim(F.col("id").cast(StringType())) != "")
        )

        # 2. Deduplicar
        sdf = sdf.dropDuplicates(["id"])

        # 3. Rellenar nulos en strings
        if "title" in sdf.columns:
            if "slug" in sdf.columns:
                sdf = sdf.withColumn("title", F.coalesce(F.col("title"), F.col("slug"), F.lit("Sin título")))
            else:
                sdf = sdf.withColumn("title", F.coalesce(F.col("title"), F.lit("Sin título")))

        if "slug" in sdf.columns:
            sdf = sdf.withColumn("slug", F.coalesce(F.col("slug"), F.col("id").cast(StringType())))

        if "description" in sdf.columns:
            # Usar el título como descripción si falta
            fallback = F.col("title") if "title" in sdf.columns else F.lit("Sin descripción")
            sdf = sdf.withColumn("description", F.coalesce(F.col("description"), fallback))

        result = sdf.toPandas()
        cleaned = initial - len(result)
        logger.info(f"  [SparkCleaner] Series: {initial:,} → {len(result):,} (eliminados {cleaned:,})")
        logger.info(f"  [SparkCleaner] Nulls corregidos → title, slug, description")
        return result
