from __future__ import annotations

from typing import Dict

import pandas as pd


class ColumnRenamer:
    """
    Aplica reglas de renombrado de columnas a un DataFrame.
    """

    def __init__(
        self,
        exact_map: Dict[str, str],
        prefix_map: Dict[str, str],
    ) -> None:
        self._exact_map = exact_map
        self._prefix_map = prefix_map

    def rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renombra columnas según mapeo exacto y por prefijo.

        :param df: DataFrame original.
        :type df: pd.DataFrame
        :return: DataFrame con columnas renombradas.
        :rtype: pd.DataFrame
        """
        renamed = {}

        for col in df.columns:
            if col in self._exact_map:
                renamed[col] = self._exact_map[col]
                continue

            for prefix, label in self._prefix_map.items():
                if col.startswith(prefix):
                    renamed[col] = f"{label}{col[len(prefix):]}"
                    break
            else:
                renamed[col] = col

        return df.rename(columns=renamed)
