"""
Orquestador: Firestore -> Transform -> Rename columns -> Google Sheets.

Este módulo:
- Conecta a Firestore (solo lectura).
- Descarga documentos a memoria.
- Transforma a dos DataFrames (servers y cameras).
- Renombra columnas a nombres “humanos” según mappings.
- Reemplaza el contenido de dos pestañas en Google Sheets.

No escribe ni modifica Firestore bajo ninguna circunstancia.
"""

from __future__ import annotations

import logging
from typing import Any, List, Mapping, Optional, Tuple
from dataclasses import replace

from src.column_mappings import default_column_mappings
from src.column_renamer import ColumnRenamer
from src.config import load_settings
from src.firestore_client import FirestoreClient
from src.logging_utils import configure_logging
from src.sheets_client import GoogleSheetsClient
from src.transformer import FirestoreToFramesTransformer, ExportFrames


DocumentItem = Tuple[str, Mapping[str, Any]]


def _collect_documents(
    client: FirestoreClient,
    collection_name: str,
    limit: Optional[int],
    logger: logging.Logger,
    log_every: int,
) -> List[DocumentItem]:
    """
    Descarga documentos Firestore a memoria (doc_id, doc_dict).

    :param client: Cliente Firestore conectado.
    :type client: FirestoreClient
    :param collection_name: Colección.
    :type collection_name: str
    :param limit: Límite de docs (None = sin límite).
    :type limit: Optional[int]
    :param logger: Logger.
    :type logger: logging.Logger
    :param log_every: Cada cuántos loguear progreso.
    :type log_every: int
    :return: Lista de documentos (doc_id, doc_dict).
    :rtype: List[DocumentItem]
    """
    documents: List[DocumentItem] = []
    count = 0

    for doc in client.iter_documents(collection_name, limit):
        count += 1

        doc_dict = doc.to_dict()
        if not doc_dict:
            continue

        documents.append((doc.id, doc_dict))

        if log_every > 0 and count % log_every == 0:
            logger.info("Leídos %s documentos...", count)

    logger.info("Total documentos leídos: %s", count)
    logger.info("Docs válidos: %s", len(documents))
    return documents


def _apply_column_mappings(frames: ExportFrames) -> ExportFrames:
    mappings = default_column_mappings()

    servers_renamer = ColumnRenamer(
        exact_map=mappings.servers_exact,
        prefix_map=mappings.prefix_map,
    )
    cameras_renamer = ColumnRenamer(
        exact_map=mappings.cameras_exact,
        prefix_map=mappings.prefix_map,
    )

    servers_df = servers_renamer.rename(frames.servers)
    cameras_df = cameras_renamer.rename(frames.cameras)

    return replace(
        frames,
        servers=servers_df,
        cameras=cameras_df
    )



def run() -> None:
    """
    Orquestador principal.

    Carga settings (.env)
    Conecta Firestore (solo lectura)
    Descarga documentos
    Transforma a DataFrames
    Renombra columnas con mappings
    Sube a Google Sheets (reemplazando pestañas)

    :return: None
    :rtype: None
    """
    # crea el logger y lo transmite a la consola
    logger: logging.Logger = configure_logging(logging.INFO)

    try:
        # Obtiene las configuraciones y variables de entorno
        settings = load_settings()
        # define que nivel mostrar del log
        logger = configure_logging(settings.log_level)

        # se conecta a la bdd
        firestore_client = FirestoreClient(settings.firebase_credentials_path)
        firestore_client.connect()

        # Obtiene los documentos de la colección
        logger.info("Conectado a Firestore, obteniendo docuementos...")
        documents = _collect_documents(
            client=firestore_client,
            collection_name=settings.firestore_collection_name,
            limit=settings.limit,
            logger=logger,
            log_every=settings.log_every,
        )

        transformer = FirestoreToFramesTransformer(settings.chile_tz)
        frames = transformer.transform(documents)

        frames = _apply_column_mappings(frames)

        sheets_client = GoogleSheetsClient(
            credentials_path=settings.sheets_credentials_path,
            sheet_id=settings.google_sheet_id,
        )

        if frames.servers.empty:
            logger.warning(
                "frames.servers vacío: no se actualiza pestaña servers."
            )
        else:
            logger.info(
                "Actualizando pestaña %s (servers)...",
                settings.servers_sheet_name,
            )
            sheets_client.replace_dataframe(
                worksheet_name=settings.servers_sheet_name,
                df=frames.servers,
            )
            logger.info(
                "OK servers: rows=%s cols=%s",
                len(frames.servers),
                len(frames.servers.columns),
            )

        if frames.cameras.empty:
            logger.warning(
                "frames.cameras vacío: no se actualiza pestaña cameras."
            )
        else:
            logger.info(
                "Actualizando pestaña %s (cameras)...",
                settings.cameras_sheet_name,
            )
            sheets_client.replace_dataframe(
                worksheet_name=settings.cameras_sheet_name,
                df=frames.cameras,
                merge_repeated_in_column="Servidor",
            )
            logger.info(
                "OK cameras: rows=%s cols=%s",
                len(frames.cameras),
                len(frames.cameras.columns),
            )

        logger.info("Sync completada OK.")

    except (FileNotFoundError, ValueError) as exc:
        logger = configure_logging(logging.ERROR)
        logger.error("%s", exc)
    except Exception:
        logger = configure_logging(logging.ERROR)
        logger.exception("Error inesperado.")


if __name__ == "__main__":
    run()
