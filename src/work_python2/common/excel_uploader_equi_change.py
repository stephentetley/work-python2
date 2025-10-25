"""
Copyright 2025 Stephen Tetley

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

import shutil
import duckdb
import pandas as pd

# Pandas is necessary so we can use `openpyxl` and write into existing 
# Excel files which have formatting we need to follow.
# Polars uses `xlsxwriter` - the docs for it state it can't modify existing
# Excel files.


    
def write_equi_change_uploads(*,
                              upload_template_path: str, 
                              dest: str,
                              con: duckdb.DuckDBPyConnection) -> None: 
    rs = con.execute("SELECT max(batch_number) FROM excel_uploader_equi_change.vw_batch_worklist;").fetchone();
    for batch_number in range(1, rs[0] + 1, 1):
        _gen_excel_upload1(upload_template_path=upload_template_path, 
                        dest=dest,
                        batch_number=batch_number,
                        con=con)

def _gen_excel_upload1(*,
                       upload_template_path: str, 
                       dest: str,
                       batch_number: int,
                       con: duckdb.DuckDBPyConnection) -> None: 
    dest = dest.replace(".xlsx", f"_batch{batch_number}.xlsx")    
    shutil.copy(upload_template_path, dest)
    with pd.ExcelWriter(dest, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        header_pandas = con.sql("SELECT * FROM excel_uploader_equi_change.vw_change_request_header;").df()
        header_pandas.to_excel(
            writer,
            sheet_name='Change Request Header',
            startcol=0,
            startrow=5,
            index=False,
            header=False)
        
        notes_query = f"""
            SELECT * FROM excel_uploader_equi_change.change_request_notes
            UNION 
            SELECT 'Batch number {batch_number}' AS usmd_note
            UNION
            SELECT 'Upload file created on ' || strftime(current_date, '%d.%m.%Y') AS usmd_note;
        """
        notes_pandas = con.sql(notes_query).df()
        notes_pandas.to_excel(
            writer,
            sheet_name='Change Request Notes',
            startcol=0,
            startrow=4,
            index=False,
            header=False)
        
        equi_query = f"""
            SELECT * EXCLUDE(batch_number)
            FROM excel_uploader_equi_change.vw_equipment_data
            WHERE batch_number = {batch_number};            
        """
        equi_pandas = con.sql(equi_query).df()
        equi_pandas.to_excel(writer,
                             sheet_name='EQ-Equipment Data',
                             startcol=0,
                             startrow=5,
                             index=False,
                             header=False)
        
        equi_char_query = f"""
            SELECT * EXCLUDE (batch_number) 
            FROM excel_uploader_equi_change.vw_classification
            WHERE batch_number = {batch_number};
        """        
        equi_chars_pandas = con.sql(equi_char_query).df()
        equi_chars_pandas.to_excel(writer,
                                   sheet_name='EQ-Classification',
                                   startcol=0,
                                   startrow=5,
                                   index=False,
                                   header=False)

