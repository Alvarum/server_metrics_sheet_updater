from __future__ import annotations

from typing import Optional
from pathlib import Path

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from gspread.utils import rowcol_to_a1


class GoogleSheetsClient:
    """
    Cliente para actualizar pestañas de Google Sheets.

    :param credentials_path: JSON service account.
    :type credentials_path: Path
    :param sheet_id: ID del spreadsheet.
    :type sheet_id: str
    """

    def __init__(
        self,
        credentials_path: Path,
        sheet_id: str
    ) -> None:
        self._credentials_path = credentials_path
        self._sheet_id = sheet_id


    def _open_sheet(self) -> gspread.Spreadsheet:
        """
        Abre el spreadsheet.

        :return: Spreadsheet.
        :rtype: gspread.Spreadsheet
        :raises FileNotFoundError: Si no existe JSON.
        """
        if not self._credentials_path.exists():
            raise FileNotFoundError(
                f"No existe: {self._credentials_path}"
            )

        gc = gspread.service_account(
            filename=str(self._credentials_path)
        )
        return gc.open_by_key(self._sheet_id)


    @staticmethod
    def _open_or_create_worksheet(
        sheet: gspread.Spreadsheet,
        title: str,
        rows: int,
        cols: int,
    ) -> gspread.Worksheet:
        """
        Abre o crea pestaña.

        :param sheet: Spreadsheet.
        :type sheet: gspread.Spreadsheet
        :param title: Nombre pestaña.
        :type title: str
        :param rows: Filas.
        :type rows: int
        :param cols: Columnas.
        :type cols: int
        :return: Worksheet.
        :rtype: gspread.Worksheet
        """
        try:
            return sheet.worksheet(title)
        except gspread.WorksheetNotFound:
            return sheet.add_worksheet(
                title=title,
                rows=rows,
                cols=cols
            )


    @staticmethod
    def _merge_repeated_in_column(
        ws: gspread.Worksheet,
        df: pd.DataFrame,
        column_name: str,
        start_row: int = 1,
        start_col: int = 1,
        has_header: bool = True,
    ) -> None:
        if df.empty:
            return

        if column_name not in df.columns:
            return

        output_rows = len(df) + (1 if has_header else 0)
        output_cols = len(df.columns)

        last_row = start_row + output_rows - 1
        last_col = start_col + output_cols - 1

        unmerge = getattr(ws, "unmerge_cells", None)
        if callable(unmerge):
            try:
                start_a1 = rowcol_to_a1(start_row, start_col)
                end_a1 = rowcol_to_a1(last_row, last_col)
                unmerge(f"{start_a1}:{end_a1}")
            except Exception:
                try:
                    start_a1 = rowcol_to_a1(1, 1)
                    end_a1 = rowcol_to_a1(ws.row_count, ws.col_count)
                    unmerge(f"{start_a1}:{end_a1}")
                except Exception:
                    pass

        col_idx = start_col + int(df.columns.get_loc(column_name))
        first_data_row = start_row + (1 if has_header else 0) + 1

        values = df[column_name].tolist()

        i = 0
        while i < len(values):
            j = i + 1
            while j < len(values) and values[j] == values[i]:
                j += 1

            if (j - i) > 1:
                start_a1 = rowcol_to_a1(first_data_row + i, col_idx)
                end_a1 = rowcol_to_a1(first_data_row + (j - 1), col_idx)
                ws.merge_cells(f"{start_a1}:{end_a1}")

            i = j



    def replace_dataframe(
        self,
        worksheet_name: str,
        df: pd.DataFrame,
        merge_repeated_in_column: Optional[str] = None,
    ) -> None:
        """
        Reemplaza contenido completo de una pestaña por un DataFrame.

        :param worksheet_name: Nombre pestaña.
        :type worksheet_name: str
        :param df: DataFrame.
        :type df: pd.DataFrame
        :return: None
        :rtype: None
        """
        sheet = self._open_sheet()

        ws = self._open_or_create_worksheet(
            sheet=sheet,
            title=worksheet_name,
            rows=max(len(df) + 10, 100),
            cols=max(len(df.columns) + 5, 20),
        )
        ws.clear()
        set_with_dataframe(
            worksheet=ws,
            dataframe=df,
            include_index=False,
            include_column_header=True,
            resize=True,
        )

        if merge_repeated_in_column is not None:
            self._merge_repeated_in_column(
                ws=ws,
                df=df,
                column_name=merge_repeated_in_column,
                start_row=1,
                start_col=1,
                has_header=True,
            )

