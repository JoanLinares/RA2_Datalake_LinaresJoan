"""
Data Transformer - Fase 2
Normalización y validación de datos antes de cargar al warehouse
Maneja: desanidación de JSON, normalización de tipos, limpieza de datos
"""
import json
import logging
from typing import List, Tuple, Any, Optional
import pandas as pd
import numpy as np

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

