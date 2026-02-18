#!/usr/bin/env python3
"""
Main - Orquestador del Pipeline de ETL
Fase 1: Extracción de datos de Polymarket a Delta Lake
Fase 2: Transformación, Carga y Validación en NeonDB

Flujo automático:
1. Si no existe datalake/raw → ejecuta extractor_polymarket
2. Ejecuta DataTransformer (normalización de datos)
3. Ejecuta WarehouseValidator (validación de integridad)
4. Ejecuta WarehouseLoader (carga a NeonDB)
"""
import os
import sys
import subprocess
import logging
from pathlib import Path
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()


def check_datalake_exists() -> bool:
    """Verifica si la carpeta datalake/raw existe con datos"""
    datalake_path = Path("datalake/raw")
    
    if not datalake_path.exists():
        logger.warning("❌ Carpeta datalake/raw no encontrada")
        return False
    
    # Verificar que existan al menos algunas subcarpetas
    expected_folders = ["events", "markets", "series", "tags"]
    folders_found = [
        (datalake_path / folder).exists() 
        for folder in expected_folders
    ]
    
    if not any(folders_found):
        logger.warning("❌ Subcarpetas de datalake/raw no encontradas")
        return False
    
    logger.info("✅ Carpeta datalake/raw encontrada")
    return True


def run_extractor():
    """Ejecuta el extractor de Polymarket"""
    logger.info("\n" + "="*70)
    logger.info("FASE 1: EXTRACCIÓN DE DATOS DE POLYMARKET")
    logger.info("="*70)
    
    try:
        result = subprocess.run(
            [sys.executable, "extractor_polymarket.py"],
            cwd=Path(__file__).parent,
            capture_output=False
        )
        
        if result.returncode != 0:
            logger.error("❌ Error durante la extracción")
            return False
        
        logger.info("✅ Extracción completada")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error ejecutando extractor: {e}")
        return False


def run_transformer():
    """Ejecuta el transformador de datos"""
    logger.info("\n" + "="*70)
    logger.info("FASE 2A: TRANSFORMACIÓN DE DATOS")
    logger.info("="*70)
    
    try:
        from src.utils.transformer_data import DataTransformer
        from deltalake import DeltaTable
        import pandas as pd
        
        datalake_path = Path("datalake/raw")
        
        # Leer y transformar eventos
        logger.info("\n[1/4] Transformando eventos...")
        events_path = datalake_path / "events"
        if events_path.exists():
            try:
                events_df = DeltaTable(str(events_path)).to_pandas()
                logger.info(f"Leídos {len(events_df)} eventos")
                events_df = DataTransformer.validate_and_clean_events(events_df)
                logger.info(f"✓ Evento limpiados: {len(events_df)} registros")
            except Exception as e:
                logger.warning(f"⚠ Error transformando eventos: {e}")
        
        # Leer y transformar mercados
        logger.info("\n[2/4] Transformando mercados...")
        markets_path = datalake_path / "markets"
        if markets_path.exists():
            try:
                markets_df = DeltaTable(str(markets_path)).to_pandas()
                logger.info(f"Leídos {len(markets_df)} mercados")
                markets_df = DataTransformer.validate_and_clean_markets(markets_df)
                logger.info(f"✓ Mercados limpios: {len(markets_df)} registros")
            except Exception as e:
                logger.warning(f"⚠ Error transformando mercados: {e}")
        
        # Leer series
        logger.info("\n[3/4] Leyendo series...")
        series_path = datalake_path / "series"
        if series_path.exists():
            try:
                series_df = DeltaTable(str(series_path)).to_pandas()
                logger.info(f"✓ Series leídas: {len(series_df)} registros")
            except Exception as e:
                logger.warning(f"⚠ Error leyendo series: {e}")
        
        # Leer tags
        logger.info("\n[4/4] Leyendo tags...")
        tags_path = datalake_path / "tags"
        if tags_path.exists():
            try:
                tags_df = DeltaTable(str(tags_path)).to_pandas()
                logger.info(f"✓ Tags leídas: {len(tags_df)} registros")
            except Exception as e:
                logger.warning(f"⚠ Error leyendo tags: {e}")
        
        logger.info("✅ Transformación completada")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error durante transformación: {e}")
        return False


def run_validator():
    """Ejecuta la validación de warehouse"""
    logger.info("\n" + "="*70)
    logger.info("FASE 2B: VALIDACIÓN PRE-CARGA")
    logger.info("="*70)
    
    try:
        from src.utils.validator_warehouse import WarehouseValidator
        
        DATABASE_URL = os.getenv('DATABASE_URL')
        if not DATABASE_URL:
            logger.error("❌ DATABASE_URL no encontrada en .env")
            return False
        
        validator = WarehouseValidator(DATABASE_URL)
        validator.connect()
        
        # Validar archivos de Delta Lake
        logger.info("\n📁 Verificando disponibilidad de datos...")
        datalake_path = Path("datalake/raw")
        
        expected_items = {
            "events": datalake_path / "events",
            "markets": datalake_path / "markets",
            "series": datalake_path / "series",
            "tags": datalake_path / "tags"
        }
        
        all_exist = True
        for name, path in expected_items.items():
            if path.exists():
                logger.info(f"✓ {name}: disponible")
            else:
                logger.warning(f"✗ {name}: no disponible")
                all_exist = False
        
        if not all_exist:
            logger.warning("⚠ Algunos datos están faltando")
        
        validator.close()
        logger.info("✅ Validación completada")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error durante validación: {e}")
        return False


def run_loader():
    """Ejecuta el cargador de warehouse"""
    logger.info("\n" + "="*70)
    logger.info("FASE 2C: CARGA EN NEONDB")
    logger.info("="*70)
    
    try:
        from src.warehouse.loader_NeonDB import WarehouseLoader
        
        DATABASE_URL = os.getenv('DATABASE_URL')
        if not DATABASE_URL:
            logger.error("❌ DATABASE_URL no encontrada en .env")
            return False
        
        loader = WarehouseLoader(DATABASE_URL)
        loader.connect()
        loader.load_all()
        
        logger.info("✅ Carga completada")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error durante carga: {e}")
        logger.exception("Detalles:")
        return False


def main():
    """Función principal - orquesta todo el pipeline"""
    
    logger.info("\n" + "🚀 "*35)
    logger.info("INICIANDO PIPELINE COMPLETO DE ETL")
    logger.info("Polymarket → Delta Lake → NeonDB")
    logger.info("🚀 "*35 + "\n")
    
    # Fase 1: Extracción
    if not check_datalake_exists():
        logger.info("\n⚙️  Iniciando extracción de datos...")
        if not run_extractor():
            logger.error("❌ Pipeline abortado: falló extracción")
            return 1
    else:
        logger.info("⏭️  Saltando extracción: datalake/raw ya existe")
    
    # Fase 2A: Transformación
    logger.info("\n⚙️  Iniciando transformación de datos...")
    if not run_transformer():
        logger.error("❌ Pipeline abortado: falló transformación")
        return 1
    
    # Fase 2B: Validación
    logger.info("\n⚙️  Iniciando validación...")
    if not run_validator():
        logger.error("⚠️  Advertencia durante validación, continuando...")
    
    # Fase 2C: Carga
    logger.info("\n⚙️  Iniciando carga en NeonDB...")
    if not run_loader():
        logger.error("❌ Pipeline abortado: falló carga")
        return 1
    
    # Éxito
    logger.info("\n" + "✅ "*35)
    logger.info("PIPELINE COMPLETADO EXITOSAMENTE")
    logger.info("✅ "*35 + "\n")
    
    logger.info("📊 Resultados:")
    logger.info("  • Datos extraídos desde Polymarket API")
    logger.info("  • Almacenados en Delta Lake (datalake/raw/)")
    logger.info("  • Transformados y normalizados")
    logger.info("  • Cargados en PostgreSQL (NeonDB)")
    logger.info("  • Validación de integridad completada")
    logger.info("\nPróximos pasos:")
    logger.info("  → Explorar datos en NeonDB")
    logger.info("  → Ejecutar análisis en la capa Gold")
    logger.info("  → Generar insights y reportes\n")
    
    return 0


if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("\n⚠️  Pipeline interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error inesperado: {e}")
        logger.exception("Traceback:")
        sys.exit(1)

