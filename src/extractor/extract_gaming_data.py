"""
Gaming Data Extraction - Extrae y limpia datos de gaming del Delta Lake
Prepara los datos para análisis en Tableau y carga en NeonDB
"""
import sys
import logging
from pathlib import Path
from deltalake import DeltaTable
import pandas as pd

# Agregar src al path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from utils.transformer_data import DataTransformer

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_gaming_data():
    """
    Extrae datos de gaming del Delta Lake, limpia y prepara para Tableau
    """
    
    logger.info("="*100)
    logger.info("EXTRACCION DE DATOS GAMING PARA TABLEAU")
    logger.info("="*100)
    
    try:
        # PASO 1: Leer mercados del Delta Lake
        logger.info("\n[PASO 1] Leyendo datos de mercados desde Delta Lake...")
        delta_path = Path("datalake/raw/markets")
        
        if not delta_path.exists():
            logger.error(f"Delta Lake no encontrado en {delta_path}")
            return False
        
        dt = DeltaTable(str(delta_path))
        df_markets = dt.to_pandas()
        logger.info(f"✓ {len(df_markets):,} mercados cargados del Delta Lake")
        
        # PASO 2: Limpiar y filtrar datos de gaming
        logger.info("\n[PASO 2] Filtrando y limpiando datos GAMING...")
        df_gaming = DataTransformer.validate_and_clean_gaming_markets(df_markets)
        
        if len(df_gaming) == 0:
            logger.error("❌ No se encontraron mercados de gaming")
            return False
        
        logger.info(f"✓ {len(df_gaming):,} mercados de gaming preparados")
        
        # PASO 3: Generar resumen estadístico
        logger.info("\n[PASO 3] Generando resumen estadístico...")
        summary = DataTransformer.generate_gaming_summary(df_gaming)
        
        logger.info(f"\n📊 RESUMEN DE GAMING:")
        logger.info(f"   Total mercados: {summary['total_markets']:,}")
        logger.info(f"   Mercados activos: {summary['active_markets']:,}")
        logger.info(f"   Mercados cerrados: {summary['closed_markets']:,}")
        logger.info(f"   Volumen total: ${summary['total_volume']:,.2f}")
        logger.info(f"   Volumen promedio: ${summary['avg_volume']:,.2f}")
        logger.info(f"   Liquidez total: ${summary['total_liquidity']:,.2f}")
        logger.info(f"   Liquidez promedio: ${summary['avg_liquidity']:,.2f}")
        
        logger.info(f"\n🎮 TIPOS DE JUEGO:")
        for game_type, count in summary['gaming_types'].items():
            pct = (count / summary['total_markets']) * 100
            logger.info(f"   {game_type:25} {count:6,} mercados ({pct:5.1f}%)")
        
        logger.info(f"\n🎯 TIPOS DE APUESTA:")
        for bet_type, count in summary['bet_types'].items():
            pct = (count / summary['total_markets']) * 100
            logger.info(f"   {bet_type:25} {count:6,} mercados ({pct:5.1f}%)")
        
        logger.info(f"\n📈 DISTRIBUCIÓN DE OUTCOMES:")
        logger.info(f"   2 outcomes (Si/No):     {summary['outcome_types']['2_outcomes']:6,}")
        logger.info(f"   3 outcomes:             {summary['outcome_types']['3_outcomes']:6,}")
        logger.info(f"   4+ outcomes:            {summary['outcome_types']['4plus_outcomes']:6,}")
        
        # PASO 4: Guardar datos limpios
        logger.info("\n[PASO 4] Guardando datos limpios...")
        
        output_dir = Path("datalake/processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Guardar como CSV para importar en Tableau
        csv_path = output_dir / "gaming_markets_clean.csv"
        df_gaming.to_csv(csv_path, index=False)
        logger.info(f"✓ CSV guardado: {csv_path}")
        
        # Guardar como Parquet (formato columnar más eficiente)
        parquet_path = output_dir / "gaming_markets_clean.parquet"
        df_gaming.to_parquet(parquet_path, index=False)
        logger.info(f"✓ Parquet guardado: {parquet_path}")
        
        # Guardar top mercados por volumen
        logger.info("\n[PASO 5] Identificando top mercados por volumen...")
        top_markets = df_gaming.nlargest(50, 'volume')[
            ['id', 'question', 'gaming_type', 'bet_type', 'volume', 'liquidity', 
             'active', 'closed', 'outcomes_list']
        ]
        
        top_path = output_dir / "gaming_top_markets.csv"
        top_markets.to_csv(top_path, index=False)
        logger.info(f"✓ Top 50 mercados guardados: {top_path}")
        
        logger.info("\n" + "="*100)
        logger.info("EXTRACCION COMPLETADA")
        logger.info("="*100)
        logger.info(f"\n✅ Archivos generados (listos para Tableau):")
        logger.info(f"   1. {csv_path.name} - Todos los mercados de gaming ({len(df_gaming):,})")
        logger.info(f"   2. {parquet_path.name} - Formato optimizado")
        logger.info(f"   3. {top_path.name} - Top 50 mercados por volumen")
        
        logger.info(f"\nPasos siguientes:")
        logger.info(f"   1. Importar los CSV/Parquet en Tableau")
        logger.info(f"   2. Crear visualizaciones por gaming_type y bet_type")
        logger.info(f"   3. Análisis de volumen, liquidez y distribución de outcomes")
        logger.info(f"   4. (Opcional) Cargar en NeonDB para análisis más complejos")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en extracción: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = extract_gaming_data()
    exit_code = 0 if success else 1
    sys.exit(exit_code)
