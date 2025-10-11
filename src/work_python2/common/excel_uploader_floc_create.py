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



# No batching for flocs - dependcies from child to parent flocs 
# mean we should let the user batch by hand...
def write_excel_upload(*,
                       upload_template_path: str, 
                       dest: str,
                       con: duckdb.DuckDBPyConnection) -> None: 
    shutil.copy(upload_template_path, dest)
    with pd.ExcelWriter(dest, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        # TODO - dont bother with _write_tab
        def _write_tab(*, sel_stmt: str, sheet_name: str) -> None:
            pandas_df = con.sql(sel_stmt).df()
            pandas_df.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    startcol=0,
                    startrow=5,
                    index=False,
                    header=False,
                )  
        header_pandas = con.sql("SELECT * FROM excel_uploader_floc_create.vw_change_request_header;").df()
        header_pandas.to_excel(writer,
                               sheet_name='Change Request Header',
                               startcol=0,
                               startrow=5,
                               index=False,
                               header=False) 
        
        notes_pandas = con.sql("SELECT * FROM excel_uploader_floc_create.change_request_notes;").df()
        notes_pandas.to_excel(writer,
                              sheet_name='Change Request Notes',
                              startcol=0,
                              startrow=4,
                              index=False,
                              header=False) 
        
        floc_pandas = con.sql("SELECT * FROM excel_uploader_floc_create.vw_functional_location;").df()
        floc_pandas.to_excel(writer,
                             sheet_name='FLOC-Functional Location',
                             startcol=0,
                             startrow=5,
                             index=False,
                             header=False) 
        
        floc_chars_pandas = con.sql("SELECT * FROM excel_uploader_floc_create.vw_classification;").df()
        floc_chars_pandas.to_excel(writer,
                                   sheet_name='FLOC-Classification',
                                   startcol=0,
                                   startrow=5,
                                   index=False,
                                   header=False) 

