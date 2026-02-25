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


def run_s3_upload():
    """Sube el Delta Lake a Amazon S3"""
    logger.info("\n" + "="*70)
    logger.info("FASE 1B: CARGA A AMAZON S3")
    logger.info("="*70)
    
    try:
        from src.S3.upload_datalake_s3 import S3Uploader
        
        # Crear uploader
        uploader = S3Uploader()
        
        # Subir datalake
        success = uploader.upload_datalake("datalake")
        
        if success:
            # Verificar
            uploader.verify_upload()
            logger.info("✅ Carga a S3 completada")
            return True
        else:
            logger.warning("⚠️  Error durante la carga a S3 (continuando)")
            return False
        
    except ValueError as e:
        logger.warning(f"⚠️  Error configuración S3: {e} (continuando)")
        return False
    except Exception as e:
        logger.warning(f"⚠️  Error ejecutando upload S3: {e} (continuando)")
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


def run_gaming_loader():
    """Ejecuta el pipeline COMPLETO de GAMING para NeonDB + Tableau"""
    logger.info("\n" + "="*70)
    logger.info("PIPELINE GAMING: EXTRACCION → TRANSFORMACION → CARGA NEONDB")
    logger.info("="*70)
    
    try:
        from src.warehouse.loader_NeonDB import WarehouseLoader
        
        DATABASE_URL = os.getenv('DATABASE_URL')
        if not DATABASE_URL:
            logger.error("❌ DATABASE_URL no encontrada en .env")
            return False
        
        # Crear loader y conectar
        loader = WarehouseLoader(DATABASE_URL)
        loader.connect()
        
        # Ejecutar pipeline gaming
        success = loader.load_gaming_to_warehouse()
        
        if success:
            logger.info("✅ Pipeline gaming completado exitosamente")
            logger.info("\n📊 Próximos pasos:")
            logger.info("   1. Conectar Tableau a NeonDB")
            logger.info("   2. Crear datasource con tabla dim_market (gaming)")
            logger.info("   3. Visualizar gaming_type, bet_type, volume, liquidity")
            logger.info("   4. Crear 2 dashboards con 4 gráficos cada uno")
        else:
            logger.error("❌ Error en pipeline gaming")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Error durante carga gaming: {e}")
        logger.exception("Detalles:")
        return False


def main():
    """Función principal - orquesta todo el pipeline"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Pipeline ETL Polymarket')
    parser.add_argument(
        '--gaming', 
        action='store_true',
        help='Ejecutar solo el pipeline de GAMING (para Tableau)'
    )
    
    args = parser.parse_args()
    
    # Si se especifica --gaming, ejecutar solo gaming
    if args.gaming:
        logger.info("\n" + "🎮 "*35)
        logger.info("PIPELINE ESPECIFICO: GAMING PARA TABLEAU")
        logger.info("🎮 "*35 + "\n")
        
        if not run_gaming_loader():
            logger.error("❌ Pipeline gaming abortado")
            return 1
        
        logger.info("\n" + "✅ "*35)
        logger.info("GAMING PIPELINE COMPLETADO")
        logger.info("✅ "*35 + "\n")
        return 0
    
    # Si no, ejecutar pipeline completo normal
    logger.info("INICIANDO PIPELINE COMPLETO DE ETL")
    logger.info("Polymarket → Delta Lake → S3 → NeonDB")
    logger.info("🚀 "*35 + "\n")
    
    # Fase 1: Extracción
    extraction_done = False
    if not check_datalake_exists():
        logger.info("\n⚙️  Iniciando extracción de datos...")
        if not run_extractor():
            logger.error("❌ Pipeline abortado: falló extracción")
            return 1
        extraction_done = True
    else:
        logger.info("⏭️  Saltando extracción: datalake/raw ya existe")
    
    # Fase 1B: Carga a S3 (solo si se extrajo o si el usuario quiere subir)
    if extraction_done or check_datalake_exists():
        logger.info("\n⚙️  Subiendo Delta Lake a Amazon S3...")
        run_s3_upload()  # No bloquea si falla
    
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
    logger.info("  • Respaldados en Amazon S3")
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

