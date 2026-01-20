import logging
from datetime import datetime
import pytz
from src.config import config
from src.services.firestore import FirestoreService
from src.services.transformer import DataTransformer
from src.services.sheets import SheetsService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """
    Ejecuta el pipeline ETL completo.

    1. Extracción: Obtiene datos de Firestore.
    2. Transformación: Normaliza fechas y estructura datos.
    3. Carga: Escribe en Snapshot e Historial de Google Sheets.
    """
    logger.info(">>> Iniciando Pipeline ETL")

    try:
        # Configuración de Tiempos
        tz_chile = pytz.timezone(config.timezone)
        now_utc = datetime.now(pytz.utc)
        now_chile = now_utc.astimezone(tz_chile)

        str_chile = now_chile.strftime("%Y-%m-%d %H:%M:%S")
        str_utc = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

        # --- Extracción ---
        logger.info("Conectando a Firestore...")
        firestore = FirestoreService()
        raw_docs = list(firestore.get_documents())

        if not raw_docs:
            logger.warning("No hay datos disponibles. Finalizando.")
            return

        # --- Transformación ---
        logger.info("Transformando datos...")
        transformer = DataTransformer()
        # Se procesa la data sin argumentos extra (toma timestamp interno)
        datasets = transformer.process_data(raw_docs)

        # --- Carga ---
        logger.info("Conectando a Google Sheets...")
        sheets = SheetsService()

        # Procesa Servidores
        logger.info("Procesando Servidores...")
        sheets.update_snapshot(
            config.servers_config,
            datasets["servers"],
            str_chile,
            str_utc
        )
        sheets.append_history(
            config.servers_config,
            datasets["servers"],
            str_chile
        )

        # Procesa Cámaras
        logger.info("Procesando Cámaras...")
        sheets.update_snapshot(
            config.cameras_config,
            datasets["cameras"],
            str_chile,
            str_utc
        )
        sheets.append_history(
            config.cameras_config,
            datasets["cameras"],
            str_chile
        )

        logger.info("<<< Pipeline finalizado con éxito.")

    except Exception as e:
        logger.error(
            f"Error fatal en el pipeline: {e}",
            exc_info=True
        )