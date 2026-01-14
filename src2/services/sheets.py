import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from gspread.utils import rowcol_to_a1
from src2.config import config

class SheetsService:
    FORMATS = {
        "NUMBER": {"numberFormat": {"type": "NUMBER", "pattern": "0.0"}},
        "PERCENT": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}},
        "DATE_TIME": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}},
        "TEXT": {"numberFormat": {"type": "TEXT"}},
    }

    def __init__(self):
        gc = gspread.service_account(filename=config.creds_sheets)
        self.sh = gc.open_by_key(config.sheet_id)

    def update_snapshot(self, tab_config: dict, df: pd.DataFrame, time_chile, time_utc):
        """Sobrescribe la hoja principal con encabezado y datos."""
        if df.empty: return
        
        tab_name = tab_config["tab_name"]
        ws = self._get_or_create_worksheet(tab_name)
        ws.clear()

        # 1. Escribir Encabezado de Metadatos (Filas 1-3)
        header_data = [
            ["Titulo", tab_config.get("title", "Reporte")],
            ["Última actualización (Chile)", str(time_chile)],
            ["Última actualización (UTC)", str(time_utc)]
        ]
        ws.update("A1:B3", header_data)
        
        # Formato negrita al encabezado
        ws.format("A1:A3", {"textFormat": {"bold": True}})

        # 2. Escribir la Tabla de Datos (Empezando en Fila 6)
        start_row = 6
        set_with_dataframe(ws, df, row=start_row, resize=True)

        # 3. Aplicar Estilos a la Tabla
        self._apply_formats(ws, df, tab_config["columns"], start_row)

        # 4. Merge visual (si aplica)
        if "merge_column" in tab_config:
            self._merge_cells(ws, df, tab_config["columns"][tab_config["merge_column"]]["name"], start_row)

    def append_history(self, tab_config: dict, df: pd.DataFrame, time_chile):
        """Agrega filas al final de la hoja de historial."""
        if df.empty: return
        
        hist_tab_name = tab_config.get("history_tab")
        if not hist_tab_name: return

        # Preparamos el DF para historial: Agregamos timestamp de ejecución
        df_history = df.copy()
        # Insertamos la fecha de extracción al principio
        df_history.insert(0, "Fecha Extracción", time_chile)

        ws = self._get_or_create_worksheet(hist_tab_name)
        
        # Si la hoja está vacía (nueva), escribimos con cabeceras
        if len(ws.get_all_values()) == 0:
            set_with_dataframe(ws, df_history, row=1)
        else:
            # Si ya tiene datos, hacemos append (sin header)
            # Convertimos a lista de listas para gspread
            # Manejo de timestamps para que JSON sea serializable
            payload = df_history.astype(str).values.tolist()
            ws.append_rows(payload)

    def _get_or_create_worksheet(self, name):
        try:
            return self.sh.worksheet(name)
        except:
            return self.sh.add_worksheet(name, rows=100, cols=20)

    def _apply_formats(self, ws, df, columns_config, start_row_idx):
        batch = []
        final_name_map = {v["name"]: v for k, v in columns_config.items()}

        for i, col_name in enumerate(df.columns):
            if col_name in final_name_map:
                fmt_type = final_name_map[col_name].get("format", "TEXT")
                rule = self.FORMATS.get(fmt_type)
                if rule:
                    col_letter = rowcol_to_a1(1, i + 1).replace("1", "")
                    # Rango: Desde (start_row + 1) porque start_row es el header de la tabla
                    data_start = start_row_idx + 1
                    batch.append({
                        "range": f"{col_letter}{data_start}:{col_letter}{data_start + len(df)}",
                        "format": rule
                    })
        if batch: ws.batch_format(batch)

    def _merge_cells(self, ws, df, col_name, start_row_idx):
        if col_name not in df.columns: return
        col_idx = df.columns.get_loc(col_name) + 1
        values = df[col_name].tolist()
        
        # El header de la tabla está en start_row_idx
        # Los datos empiezan en start_row_idx + 1
        data_offset = start_row_idx + 1
        
        start = 0
        for i in range(1, len(values)):
            if values[i] != values[start]:
                if i - start > 1:
                    start_cell = rowcol_to_a1(start + data_offset, col_idx)
                    end_cell = rowcol_to_a1(i + data_offset - 1, col_idx)
                    ws.merge_cells(f"{start_cell}:{end_cell}")
                    ws.format(f"{start_cell}:{end_cell}", {"verticalAlignment": "MIDDLE"})
                start = i
        
        if len(values) - start > 1:
             start_cell = rowcol_to_a1(start + data_offset, col_idx)
             end_cell = rowcol_to_a1(len(values) + data_offset - 1, col_idx)
             ws.merge_cells(f"{start_cell}:{end_cell}")
             ws.format(f"{start_cell}:{end_cell}", {"verticalAlignment": "MIDDLE"})