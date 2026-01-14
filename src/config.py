from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    """
    Configuración cargada desde variables de entorno.

    :param firebase_credentials_path: JSON service account Firebase Admin.
    :type firebase_credentials_path: Path
    :param firestore_collection_name: Nombre de colección Firestore.
    :type firestore_collection_name: str
    :param sheets_credentials_path: JSON service account Google Sheets.
    :type sheets_credentials_path: Path
    :param google_sheet_id: ID del Google Sheet destino.
    :type google_sheet_id: str
    :param servers_sheet_name: Nombre pestaña servers.
    :type servers_sheet_name: str
    :param cameras_sheet_name: Nombre pestaña cameras.
    :type cameras_sheet_name: str
    :param chile_tz: Timezone IANA para Chile.
    :type chile_tz: str
    :param log_level: Nivel numérico logging.
    :type log_level: int
    :param limit: Límite opcional docs.
    :type limit: Optional[int]
    :param log_every: Frecuencia logs progreso.
    :type log_every: int
    """
    firebase_credentials_path: Path
    firestore_collection_name: str
    sheets_credentials_path: Path
    google_sheet_id: str
    servers_sheet_name: str
    cameras_sheet_name: str
    chile_tz: str
    log_level: int
    limit: Optional[int]
    log_every: int


def _require_env(name: str) -> str:
    """
    Lee una variable requerida.

    :param name: Nombre variable.
    :type name: str
    :return: Valor no vacío.
    :rtype: str
    :raises ValueError: Si falta o está vacía.
    """
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Falta variable requerida: {name}")
    return value.strip()


def _optional_int(name: str) -> Optional[int]:
    """
    Lee variable opcional como int.

    :param name: Nombre variable.
    :type name: str
    :return: int o None.
    :rtype: Optional[int]
    :raises ValueError: Si existe pero no es int.
    """
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None

    try:
        return int(raw.strip())
    except ValueError as exc:
        raise ValueError(
            f"{name} debe ser int. Valor recibido: {raw!r}"
        ) from exc


def _log_level_from_env(raw: str) -> int:
    """
    Convierte texto a logging level.

    :param raw: Texto.
    :type raw: str
    :return: Nivel numérico.
    :rtype: int
    :raises ValueError: Si no es válido.
    """
    import logging

    mapping: Dict[str, int] = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    key = raw.strip().upper()
    if key not in mapping:
        raise ValueError(
            "LOG_LEVEL inválido. Usa DEBUG/INFO/WARNING/ERROR/CRITICAL"
        )
    return mapping[key]


def _resolve_path(value: str, base_dir: Path) -> Path:
    """
    Resuelve path absoluto/relativo.

    :param value: Ruta como string.
    :type value: str
    :param base_dir: Base para relativas.
    :type base_dir: Path
    :return: Ruta resuelta.
    :rtype: Path
    """
    raw = Path(value)
    if raw.is_absolute():
        return raw
    return (base_dir / raw).resolve()


def load_settings(env_path: Optional[Path] = None) -> Settings:
    """
    Carga .env y arma Settings.

    - Si env_path se entrega: rutas relativas se resuelven desde ese .env.
    - Si no: rutas relativas se resuelven desde la carpeta del script main.

    :param env_path: Ruta opcional al .env.
    :type env_path: Optional[Path]
    :return: Settings.
    :rtype: Settings
    """
    # Define un .env opcional: si el caller lo entrega, usamos esa ruta.
    # Esto permite apuntar explícitamente a un archivo de configuración
    dotenv_path: Optional[Path] = env_path.resolve() if env_path else None

    # Carga variables de entorno desde el .env.
    # Si dotenv_path existe: pasamos su ruta como string para cargarlo.
    # Si dotenv_path es None: python-dotenv intentará usar su mecanismo
    #   estándar (dependiendo de cómo esté configurado; en general puede
    #   buscar un .env o simplemente no cargar nada).
    load_dotenv(dotenv_path=str(dotenv_path) if dotenv_path else None)

    # Define el "directorio base" desde el cual se resolverán rutas relativas.
    # La idea es que los paths en el .env puedan ser relativos y aun así
    # quedar anclados a un lugar coherente.
    if dotenv_path is not None:
        # Si nos dieron un .env explícito, usamos la carpeta donde vive ese .env
        # como base_dir, para que rutas relativas del .env sean consistentes.
        base_dir = dotenv_path.parent
    else:
        # Si no se entregó env_path, definimos base_dir en función del
        # archivo actual (este módulo), subiendo 2 niveles.
        # Esto evita depender del working directory (que cambia según ejecución).
        base_dir = Path(__file__).resolve().parents[1]

    # Obtiene la variable obligatoria FIREBASE_CREDENTIALS_PATH desde el entorno.
    # _require_env debe fallar si la variable no está definida (contrato "required").
    # Luego _resolve_path convierte ese string en Path absoluto si corresponde:
    # - Si es relativo: lo ancla en base_dir.
    # - Si es absoluto: lo deja tal cual.
    firebase_credentials_path = _resolve_path(
        _require_env("FIREBASE_CREDENTIALS_PATH"),
        base_dir=base_dir,
    )

    # Repite la misma lógica para credenciales de Google Sheets:
    # primero exige la variable, luego normaliza/resuelve el path.
    sheets_credentials_path = _resolve_path(
        _require_env("GOOGLE_SHEETS_CREDENTIALS_PATH"),
        base_dir=base_dir,
    )

    # Lee el nombre de la colección de Firestore desde el entorno.
    # Se considera obligatorio (si falta, _require_env debe levantar error).
    firestore_collection_name = _require_env("FIRESTORE_COLLECTION_NAME")

    # Lee el ID del Google Sheet desde el entorno (también obligatorio).
    google_sheet_id = _require_env("GOOGLE_SHEET_ID")

    # Lee el nombre de la hoja "servers" desde el entorno.
    # - Si no existe la variable, usa "servers" como default.
    # - strip() elimina espacios accidentales al inicio/fin (común en .env).
    servers_sheet_name = os.getenv("SERVERS_SHEET_NAME", "servers").strip()

    # Igual que arriba, pero para la hoja "cameras".
    cameras_sheet_name = os.getenv("CAMERAS_SHEET_NAME", "cameras").strip()

    # Define el timezone a usar para Chile.
    # Se permite override por .env, pero por defecto se usa America/Santiago.
    chile_tz = os.getenv("CHILE_TZ", "America/Santiago").strip()

    # Lee LOG_LEVEL del entorno (default INFO) y lo normaliza/valida.
    # _log_level_from_env típicamente convierte strings tipo "info"/"INFO"
    # a un nivel usable (enum/int) o valida que esté en una lista permitida.
    log_level = _log_level_from_env(os.getenv("LOG_LEVEL", "INFO"))

    # Lee LIMIT del entorno como entero opcional.
    # _optional_int suele devolver:
    # - int si existe y es parseable
    # - None si la variable no está o está vacía
    # y debería fallar si existe pero no es un número válido (según tu diseño).
    limit = _optional_int("LIMIT")

    # Lee LOG_EVERY como string (default "250") y limpia espacios.
    # Esto permite que el .env tenga " 250 " y aun así funcione.
    log_every_raw = os.getenv("LOG_EVERY", "250").strip()
    try:
        # Convierte el valor a int, porque este setting se usa como número
        # (por ejemplo, "loggear cada N registros").
        log_every = int(log_every_raw)
    except ValueError as exc:
        # Si no es convertible, levantamos un error claro y con contexto,
        # incluyendo el valor recibido para facilitar debugging.
        raise ValueError(
            f"LOG_EVERY debe ser int. Valor recibido: {log_every_raw!r}"
        ) from exc

    # Construye y retorna el objeto Settings con todos los campos ya validados
    # y, en el caso de rutas, ya resueltos a Path coherentes para el programa.
    return Settings(
        firebase_credentials_path=firebase_credentials_path,
        firestore_collection_name=firestore_collection_name,
        sheets_credentials_path=sheets_credentials_path,
        google_sheet_id=google_sheet_id,
        servers_sheet_name=servers_sheet_name,
        cameras_sheet_name=cameras_sheet_name,
        chile_tz=chile_tz,
        log_level=log_level,
        limit=limit,
        log_every=log_every,
    )
