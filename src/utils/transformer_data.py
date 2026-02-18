"""
Data Transformer - Fase 2
Normalización y validación de datos antes de cargar al warehouse
Maneja: desanidación de JSON, normalización de tipos, limpieza de datos, extracción de gaming
"""
import json
import logging
import re
from typing import List, Tuple, Any, Optional
from pathlib import Path
import pandas as pd
import numpy as np
from deltalake import DeltaTable

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transformador de datos para el warehouse"""
    
    @staticmethod
    def normalize_boolean(value: Any) -> Optional[bool]:
        """
        Normaliza múltiples formatos booleanos
        Soporta: True/False, 'True'/'False', 1/0, 'Yes'/'No', 'true'/'false'
        """
        if value is None or pd.isna(value):
            return None
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, (int, float)):
            return bool(int(value))
        
        if isinstance(value, str):
            lower = value.lower().strip()
            if lower in ('true', 'yes', '1', 't', 'y', 'si', 'sí'):
                return True
            elif lower in ('false', 'no', '0', 'f', 'n'):
                return False
        
        return None
    
    @staticmethod
    def normalize_numeric(value: Any) -> Optional[float]:
        """
        Normaliza números en varios formatos
        Soporta: US (123.45), European (123,45), Mixed (1.234,56)
        """
        if value is None or pd.isna(value):
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            value = str(value).strip()
            if not value:
                return None
            
            # Contar puntos y comas
            dots = value.count('.')
            commas = value.count(',')
            
            # Formato europeo: 1.234,56
            if dots > 0 and commas == 1 and value.rfind(',') > value.rfind('.'):
                value = value.replace('.', '').replace(',', '.')
            # Formato estadounidense o sin separador de miles
            elif commas > 0 and dots == 0:
                value = value.replace(',', '')
            
            try:
                return float(value)
            except ValueError:
                return None
        
        return None
    
    @staticmethod
    def clean_string(value: Any, max_length: int = 5000) -> Optional[str]:
        """Limpia y normaliza strings"""
        if value is None or pd.isna(value):
            return None
        
        value = str(value).strip()
        if not value:
            return None
        
        # Normalizar múltiples espacios
        value = ' '.join(value.split())
        
        # Remover caracteres de control
        value = ''.join(char for char in value if ord(char) >= 32 or char in '\n\t\r')
        
        # Limitar longitud
        if len(value) > max_length:
            value = value[:max_length]
        
        return value if value else None
    
    @staticmethod
    def normalize_prices(prices_str: Any) -> Optional[List[float]]:
        """
        Deserializa precios JSON
        ['0.45', '0.55'] -> [0.45, 0.55]
        """
        if not prices_str or pd.isna(prices_str):
            return None
        
        try:
            if isinstance(prices_str, str):
                # Limpiar y parsear
                prices_str = prices_str.strip()
                if prices_str.startswith('['):
                    prices_str = prices_str.replace("'", '"')
                    prices = json.loads(prices_str)
                else:
                    return None
            elif isinstance(prices_str, list):
                prices = prices_str
            else:
                return None
            
            # Convertir a floats
            result = []
            for price in prices:
                if isinstance(price, str):
                    try:
                        result.append(float(price))
                    except ValueError:
                        continue
                elif isinstance(price, (int, float)):
                    result.append(float(price))
            
            return result if result else None
        
        except Exception as e:
            logger.debug(f"Error normalizando precios: {e}")
            return None
    
    @staticmethod
    def normalize_outcomes(outcomes_str: Any) -> Optional[List[str]]:
        """
        Deserializa outcomes
        "[' YES', ' NO']" -> ['YES', 'NO']
        """
        if not outcomes_str or pd.isna(outcomes_str):
            return None
        
        try:
            if isinstance(outcomes_str, str):
                outcomes_str = outcomes_str.strip()
                if outcomes_str.startswith('['):
                    outcomes_str = outcomes_str.replace("'", '"')
                    outcomes = json.loads(outcomes_str)
                else:
                    return None
            elif isinstance(outcomes_str, list):
                outcomes = outcomes_str
            else:
                return None
            
            # Limpiar outcomes
            result = []
            for outcome in outcomes:
                if isinstance(outcome, str):
                    outcome = outcome.strip().upper()
                    if outcome:
                        result.append(outcome)
            
            return result if result else None
        
        except Exception as e:
            logger.debug(f"Error normalizando outcomes: {e}")
            return None
    
    @staticmethod
    def parse_tags(tags_str: Any) -> Optional[List[str]]:
        """
        Extrae tags de string JSON
        "['tag1', 'tag2']" -> ['tag1', 'tag2']
        """
        if not tags_str or pd.isna(tags_str):
            return None
        
        try:
            if isinstance(tags_str, str):
                tags_str = tags_str.strip()
                if tags_str.startswith('['):
                    tags_str = tags_str.replace("'", '"')
                    tags = json.loads(tags_str)
                else:
                    return None
            elif isinstance(tags_str, list):
                tags = tags_str
            else:
                return None
            
            # Limpiar tags
            result = []
            for tag in tags:
                if isinstance(tag, str):
                    tag = tag.strip().lower()
                    if tag:
                        result.append(tag)
            
            return list(set(result)) if result else None
        
        except Exception as e:
            logger.debug(f"Error parseando tags: {e}")
            return None
    
    @staticmethod
    def validate_and_clean_events(df: pd.DataFrame) -> pd.DataFrame:
        """
        Pipeline completo de validación y limpieza de eventos
        """
        logger.info("Limpiando eventos...")
        df = df.copy()
        
        # Deduplicar por ID
        initial_count = len(df)
        df = df.drop_duplicates(subset=['id'], keep='first')
        logger.info(f"Removidos {initial_count - len(df)} eventos duplicados")
        
        # Normalizar booleanos
        boolean_cols = ['active', 'closed', 'featured', 'resolved']
        for col in boolean_cols:
            if col in df.columns:
                df[col] = df[col].apply(DataTransformer.normalize_boolean)
        
        # Normalizar strings
        string_cols = ['title', 'description', 'category', 'subcategory', 
                       'ticker', 'slug', 'sport', 'resolutionSource', 'seriesSlug']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: DataTransformer.clean_string(x, max_length=2048)
                )
        
        # Normalizar fechas
        date_cols = ['startDate', 'endDate', 'creationDate', 'createdAt', 'updatedAt']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Parsear tags
        if 'tags' in df.columns:
            df['tags'] = df['tags'].apply(DataTransformer.parse_tags)
        
        logger.info(f"Eventos limpiados: {len(df)} registros válidos")
        return df
    
    @staticmethod
    def validate_and_clean_markets(df: pd.DataFrame) -> pd.DataFrame:
        """
        Pipeline completo de validación y limpieza de mercados
        """
        logger.info("Limpiando mercados...")
        df = df.copy()
        
        # Deduplicar por ID
        initial_count = len(df)
        df = df.drop_duplicates(subset=['id'], keep='first')
        logger.info(f"Removidos {initial_count - len(df)} mercados duplicados")
        
        # Normalizar booleanos
        boolean_cols = ['active', 'closed', 'featured']
        for col in boolean_cols:
            if col in df.columns:
                df[col] = df[col].apply(DataTransformer.normalize_boolean)
        
        # Normalizar strings
        string_cols = ['question', 'marketType', 'slug', 'category', 
                       'subcategory', 'resolutionSource', 'description']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: DataTransformer.clean_string(x, max_length=2048)
                )
        
        # Normalizar números
        numeric_cols = [
            'volume', 'volume24hr', 'volume1wk', 'volume1mo', 'volume1yr',
            'liquidity', 'liquidityAmm', 'liquidityClob', 'lastTradePrice',
            'bestBid', 'bestAsk', 'spread', 'openInterest', 'fee'
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].apply(DataTransformer.normalize_numeric)
        
        # Normalizar fechas
        date_cols = ['endDate', 'createdAt', 'updatedAt']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Deserializar outcomes
        if 'outcomes' in df.columns:
            df['outcomes_list'] = df['outcomes'].apply(DataTransformer.normalize_outcomes)
        
        # Deserializar precios
        numeric_cols_for_prices = ['prices']
        for col in numeric_cols_for_prices:
            if col in df.columns:
                df[col] = df[col].apply(DataTransformer.normalize_prices)
        
        logger.info(f"Mercados limpios: {len(df)} registros válidos")
        return df
    
    @staticmethod
    def extract_gaming_from_datalake(datalake_path: str = "datalake/raw/markets") -> pd.DataFrame:
        """
        EXTRAE datos de gaming directamente del Delta Lake
        Punto de entrada para el pipeline: loader_NeonDB -> transformer -> validator -> NeonDB
        
        Args:
            datalake_path: Ruta al Delta Lake de mercados
        
        Returns:
            DataFrame con mercados de gaming filtrados y listos para transformar
        """
        logger.info("="*100)
        logger.info("EXTRAYENDO DATOS GAMING DEL DELTA LAKE")
        logger.info("="*100)
        
        try:
            # Leer Delta Lake
            delta_path = Path(datalake_path)
            if not delta_path.exists():
                logger.error(f"Delta Lake no encontrado en {delta_path}")
                return pd.DataFrame()
            
            logger.info(f"\n[EXTRACCION] Leyendo Delta Lake desde {delta_path}...")
            dt = DeltaTable(str(delta_path))
            df = dt.to_pandas()
            logger.info(f"✓ {len(df):,} mercados cargados")
            
            # Filtrar gaming
            logger.info(f"\n[FILTRANDO] Aplicando filtros de gaming...")
            gaming_keywords = ['game', 'gaming', 'esports', 'esport', 'stream', 'twitch', 
                              'dota', 'cs:go', 'valorant', 'fortnite', 'minecraft', 
                              'overwatch', 'league', 'lol', 'nba 2k', 'madden', 'fifa', 'nfl']
            
            df['question_lower'] = df['question'].str.lower()
            gaming_mask = df['question_lower'].str.contains('|'.join(gaming_keywords), regex=True, na=False)
            df_gaming = df[gaming_mask].copy()
            
            logger.info(f"✓ {len(df_gaming):,} mercados de gaming encontrados")
            logger.info(f"   ({(len(df_gaming)/len(df)*100):.1f}% del total)")
            
            return df_gaming
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo del Delta Lake: {e}", exc_info=True)
            return pd.DataFrame()
    
    @staticmethod
    def extract_gaming_type(question: str) -> Optional[str]:
        """
        Extrae el tipo de juego/esports del título del mercado
        Retorna: 'DOTA', 'Valorant', 'CS:GO', 'League of Legends', 'Fortnite', etc.
        """
        if not question or pd.isna(question):
            return None
        
        question_lower = str(question).lower()
        
        # Mapeo de keywords a tipos de juego
        game_mapping = {
            'DOTA': ['dota', 'dota 2', 'dota2'],
            'Valorant': ['valorant'],
            'CS:GO': ['cs:go', 'csgo', 'counter-strike'],
            'League of Legends': ['league of legends', 'lol', 'leagueoflegends'],
            'Fortnite': ['fortnite'],
            'Minecraft': ['minecraft'],
            'Overwatch': ['overwatch'],
            'Apex Legends': ['apex', 'apex legends'],
            'Call of Duty': ['call of duty', 'cod'],
            'Hearthstone': ['hearthstone'],
            'StarCraft': ['starcraft', 'starcraft 2', 'sc2'],
            'Dota2': ['the international', 'ti8', 'ti9', 'ti10', 'ti11'],
            'Fighting Games': ['fighting', 'street fighter', 'tekken', 'mortal kombat'],
            'Esports': ['esports', 'esport', 'tournament', 'champion'],
            'Streaming': ['twitch', 'youtube', 'streamer', 'streaming'],
        }
        
        for game_type, keywords in game_mapping.items():
            for keyword in keywords:
                if keyword in question_lower:
                    return game_type
        
        # Si solo dice "game" o "gaming", retornar genérico
        if 'game' in question_lower or 'gaming' in question_lower:
            return 'Gaming'
        
        return None
    
    @staticmethod
    def extract_bet_type(question: str) -> Optional[str]:
        """
        Extrae el tipo de apuesta
        Retorna: 'Match Winner', 'Spread', 'Over/Under', 'Prop Bet', etc.
        """
        if not question or pd.isna(question):
            return None
        
        question_lower = str(question).lower()
        
        # Detectar tipos de apuestas
        if 'will win' in question_lower or 'who will win' in question_lower:
            return 'Match Winner'
        elif 'spread' in question_lower or 'by more than' in question_lower or 'by less than' in question_lower:
            return 'Spread'
        elif 'over' in question_lower and 'under' in question_lower:
            return 'Over/Under'
        elif 'total' in question_lower and ('point' in question_lower or 'kill' in question_lower):
            return 'Over/Under'
        elif 'first' in question_lower and 'win' in question_lower:
            return 'First Blood'
        elif 'mvp' in question_lower or 'best player' in question_lower:
            return 'MVP/Best Player'
        elif 'map' in question_lower or 'round' in question_lower:
            return 'Round/Map Winner'
        else:
            return 'Prop Bet'
    
    @staticmethod
    def validate_and_clean_gaming_markets(df: pd.DataFrame) -> pd.DataFrame:
        """
        Pipeline completo para limpiar y preparar mercados de GAMING para análisis
        Filtra solo gaming/esports y extrae información relevante
        """
        logger.info("Iniciando limpieza de mercados GAMING...")
        df = df.copy()
        
        # PASO 1: Filtrar solo Gaming/Esports
        gaming_keywords = ['game', 'gaming', 'esports', 'esport', 'stream', 'twitch', 
                          'dota', 'cs:go', 'valorant', 'fortnite', 'minecraft', 
                          'overwatch', 'league', 'lol', 'nba 2k', 'madden', 'fifa', 'nfl']
        
        df['question_lower'] = df['question'].str.lower()
        gaming_mask = df['question_lower'].str.contains('|'.join(gaming_keywords), regex=True, na=False)
        df = df[gaming_mask].copy()
        
        initial_count = len(df)
        logger.info(f"Mercados GAMING encontrados: {initial_count}")
        
        # PASO 2: Deduplicar por ID
        dup_count = len(df[df.duplicated(subset=['id'], keep=False)])
        df = df.drop_duplicates(subset=['id'], keep='first')
        logger.info(f"Duplicados removidos: {dup_count - (dup_count - len(df))}")
        
        # PASO 3: Extraer características de gaming
        df['gaming_type'] = df['question'].apply(DataTransformer.extract_gaming_type)
        df['bet_type'] = df['question'].apply(DataTransformer.extract_bet_type)
        
        # PASO 4: Normalizar booleanos
        boolean_cols = ['active', 'closed', 'featured']
        for col in boolean_cols:
            if col in df.columns:
                df[col] = df[col].apply(DataTransformer.normalize_boolean)
        
        # PASO 5: Normalizar strings
        df['question'] = df['question'].apply(
            lambda x: DataTransformer.clean_string(x, max_length=500)
        )
        df['description'] = df['description'].apply(
            lambda x: DataTransformer.clean_string(x, max_length=2000)
        ) if 'description' in df.columns else None
        
        # PASO 6: Normalizar números (volumen, liquidez, precios)
        numeric_cols = [
            'volume', 'volume24hr', 'volume1wk', 'volume1mo', 'volume1yr',
            'liquidity', 'liquidityAmm', 'liquidityClob', 'lastTradePrice',
            'bestBid', 'bestAsk', 'spread'
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].apply(DataTransformer.normalize_numeric)
        
        # PASO 7: Normalizar fechas
        date_cols = ['endDate', 'createdAt', 'updatedAt', 'startDate']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # PASO 8: Deserializar outcomes (Yes/No, Team A/Team B, etc.)
        if 'outcomes' in df.columns:
            df['outcomes_list'] = df['outcomes'].apply(DataTransformer.normalize_outcomes)
            df['outcome_count'] = df['outcomes_list'].apply(
                lambda x: len(x) if isinstance(x, list) else 0
            )
        
        # PASO 9: Extraer campo de precios si existe
        if 'outcomePrices' in df.columns:
            df['prices_list'] = df['outcomePrices'].apply(DataTransformer.normalize_prices)
        
        # PASO 10: Crear campo de categoría simplificada
        df['category_simplified'] = 'Gaming'
        
        # PASO 11: Seleccionar columnas relevantes para análisis
        relevant_cols = [
            'id', 'question', 'gaming_type', 'bet_type', 'category', 'subcategory',
            'volume', 'volume24hr', 'volume1wk', 'liquidity', 'liquidityAmm', 'liquidityClob',
            'lastTradePrice', 'bestBid', 'bestAsk', 'spread',
            'outcomes_list', 'outcome_count', 'prices_list',
            'active', 'closed', 'featured',
            'createdAt', 'updatedAt', 'endDate', 'startDate',
            'slug', 'marketType', 'description'
        ]
        
        # Mantener solo columnas que existan
        available_cols = [col for col in relevant_cols if col in df.columns]
        df = df[available_cols].copy()
        
        # PASO 12: Remover filas sin volumen o con datos vacíos
        if 'volume' in df.columns:
            initial_len = len(df)
            df = df[df['volume'].notna()].copy()
            logger.info(f"Mercados sin volumen removidos: {initial_len - len(df)}")
        
        logger.info(f"Mercados GAMING limpios finales: {len(df)} registros válidos")
        logger.info(f"Tipos de juego encontrados: {df['gaming_type'].nunique()}")
        logger.info(f"Distribución: {dict(df['gaming_type'].value_counts())}")
        
        return df
    
    @staticmethod
    def generate_gaming_summary(df: pd.DataFrame) -> dict:
        """
        Genera un resumen estadístico de los datos de gaming
        Útil para validación pre-carga
        """
        summary = {
            'total_markets': len(df),
            'gaming_types': dict(df['gaming_type'].value_counts()),
            'bet_types': dict(df['bet_type'].value_counts()) if 'bet_type' in df.columns else {},
            'active_markets': int(df['active'].sum()) if 'active' in df.columns else 0,
            'closed_markets': int(df['closed'].sum()) if 'closed' in df.columns else 0,
            'total_volume': float(df['volume'].sum()) if 'volume' in df.columns else 0,
            'avg_volume': float(df['volume'].mean()) if 'volume' in df.columns else 0,
            'total_liquidity': float(df['liquidity'].sum()) if 'liquidity' in df.columns else 0,
            'avg_liquidity': float(df['liquidity'].mean()) if 'liquidity' in df.columns else 0,
            'outcome_types': {
                '2_outcomes': int((df['outcome_count'] == 2).sum()) if 'outcome_count' in df.columns else 0,
                '3_outcomes': int((df['outcome_count'] == 3).sum()) if 'outcome_count' in df.columns else 0,
                '4plus_outcomes': int((df['outcome_count'] >= 4).sum()) if 'outcome_count' in df.columns else 0,
            }
        }
        return summary
    
    @staticmethod
    def pipeline_complete_gaming(datalake_path: str = "datalake/raw/markets") -> Tuple[pd.DataFrame, dict]:
        """
        PIPELINE COMPLETO DE GAMING
        Integra: Extracción -> Limpieza -> Resumen
        
        Función principal que se llama desde loader_NeonDB.py
        
        Returns:
            (df_gaming_limpio, summary_stats)
        """
        logger.info("\n" + "="*100)
        logger.info("INICIANDO PIPELINE COMPLETO GAMING")
        logger.info("="*100)
        
        try:
            # PASO 1: Extraer del Delta Lake
            logger.info("\n[PASO 1/3] EXTRACCION DEL DELTA LAKE")
            df_raw = DataTransformer.extract_gaming_from_datalake(datalake_path)
            
            if df_raw.empty:
                logger.error("No se encontraron datos de gaming")
                return pd.DataFrame(), {}
            
            # PASO 2: Limpiar y transformar
            logger.info("\n[PASO 2/3] LIMPIEZA Y TRANSFORMACION")
            df_clean = DataTransformer.validate_and_clean_gaming_markets(df_raw)
            
            if df_clean.empty:
                logger.error("DataFrame vacío después de limpieza")
                return pd.DataFrame(), {}
            
            # PASO 3: Generar resumen
            logger.info("\n[PASO 3/3] GENERANDO RESUMEN")
            summary = DataTransformer.generate_gaming_summary(df_clean)
            
            logger.info(f"\n✅ PIPELINE COMPLETADO EXITOSAMENTE")
            logger.info(f"   Total mercados gaming: {summary['total_markets']:,}")
            logger.info(f"   Mercados activos: {summary['active_markets']:,}")
            logger.info(f"   Volumen total: ${summary['total_volume']:,.2f}")
            
            return df_clean, summary
            
        except Exception as e:
            logger.error(f"❌ Error en pipeline gaming: {e}", exc_info=True)
            return pd.DataFrame(), {}
    
    
    @staticmethod
    def extract_event_tag_relations(events_df: pd.DataFrame) -> List[Tuple[str, str]]:
        """
        Extrae relaciones entre eventos y tags
        Returns: List[(event_id, tag_name), ...]
        """
        relations = []
        
        if 'id' not in events_df.columns or 'tags' not in events_df.columns:
            return relations
        
        for _, row in events_df.iterrows():
            event_id = row.get('id')
            if pd.isna(event_id):
                continue
            
            tags = row.get('tags')
            if not tags or pd.isna(tags):
                continue
            
            if isinstance(tags, list):
                for tag in tags:
                    if tag:
                        relations.append((str(event_id), str(tag).lower()))
        
        return relations
    
    @staticmethod
    def extract_market_event_relations(markets_df: pd.DataFrame) -> List[Tuple[str, str]]:
        """
        Extrae relaciones entre mercados y eventos
        Returns: List[(market_id, event_id), ...]
        """
        relations = []
        
        if 'id' not in markets_df.columns or 'events' not in markets_df.columns:
            return relations
        
        for _, row in markets_df.iterrows():
            market_id = row.get('id')
            if pd.isna(market_id):
                continue
            
            events_str = row.get('events')
            if not events_str or pd.isna(events_str):
                continue
            
            try:
                if isinstance(events_str, str):
                    events_str = events_str.strip()
                    if events_str.startswith('['):
                        events_str = events_str.replace("'", '"')
                        event_ids = json.loads(events_str)
                    else:
                        continue
                elif isinstance(events_str, list):
                    event_ids = events_str
                else:
                    continue
                
                for event_id in event_ids:
                    if event_id and pd.notna(event_id):
                        relations.append((str(market_id), str(event_id)))
            
            except Exception as e:
                logger.debug(f"Error extrayendo relación para mercado {market_id}: {e}")
        
        return relations

