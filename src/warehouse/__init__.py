"""
Data Warehouse Module - Fase 2
Carga datos desde Delta Lake a PostgreSQL (NeonDB) con modelo dimensional
"""
from .loader_NeonDB import WarehouseLoader
from ..utils.transformer_data import DataTransformer
from ..utils.validator_warehouse import WarehouseValidator

__all__ = ['WarehouseLoader', 'DataTransformer', 'WarehouseValidator']

# Punto de entrada para ejecución directa
if __name__ == '__main__':
    import os
    from pathlib import Path
    from dotenv import load_dotenv
    
    # Cargar variables de entorno
    load_dotenv()
    
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in environment variables")
    
    # Ejecutar warehouse loader
    loader_NeonDB = WarehouseLoader(DATABASE_URL)
    loader_NeonDB.connect()
    loader_NeonDB.load_all()
