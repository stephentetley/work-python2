"""
Copyright 2023 Stephen Tetley

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import duckdb
import polars as pl
import xlsxwriter

yellow_bold_header_format={
    'bold': True,
    'text_wrap': False,
    'align': 'left',
    'fg_color': '#FFFF00',
    'border': None}



def write_sql_query_to_excel(*, 
                             select_query: str,
                             con: duckdb.DuckDBPyConnection,
                             workbook: xlsxwriter.Workbook, 
                             sheet_name: str, 
                             column_formats: dict = None, 
                             header_format: dict = None) -> None:
    if not header_format:
        header_format = yellow_bold_header_format
    if not column_formats:
        column_formats = {}
    df = con.execute(query=select_query).pl()
    df.write_excel(workbook, 
                   sheet_name,
                   header_format=header_format, 
                   freeze_panes=(1, 0),
                   autofit=True, 
                   column_formats = column_formats)


