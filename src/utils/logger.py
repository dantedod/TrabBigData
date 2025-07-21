# src/utils/logger.py

import logging
import sys

def setup_logger(name):
    """
    Configura e retorna um logger com formatação padrão para ser usado em todo o projeto.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
    
    return logger