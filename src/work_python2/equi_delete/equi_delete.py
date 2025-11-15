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

import os
import duckdb
import tempfile
import shutil
import work_python2.common.duckdb_utils as duckdb_utils
import work_python2.common.excel_uploader_equi_change as excel_uploader_equi_change


def run(*, 
        worklist_path: str,
        eu_equi_change_template: str,
        dispose_xlsx_output_file: str | None = None, 
        change_rec_name: str | None = None, 
        change_rec_number: int | None = None) -> None: 
    working_dir = tempfile.mkdtemp(prefix='equi_delete_')
    duckdb_del_path = os.path.normpath(os.path.join(working_dir, 'equi_delete_db.duckdb'))
    con = duckdb.connect(database=duckdb_del_path)
    
    _exec_equi_delete(worklist_path=worklist_path, 
                      con=con)

    if change_rec_name:
        insert_stmt = f"""
            INSERT INTO excel_uploader_equi_change.change_request_header 
                BY POSITION(change_request_decription)
                VALUES ('{change_rec_name}');
        """
        con.execute(insert_stmt)

    elif change_rec_number:
        insert_stmt = f"""
            INSERT INTO excel_uploader_equi_change.change_request_header 
                BY POSITION(usmd_crequest)
                VALUES ('{change_rec_number}');
        """
        con.execute(insert_stmt)

    if dispose_xlsx_output_file:
        excel_uploader_equi_change.write_equi_change_uploads(upload_template_path=eu_equi_change_template,
                                                             dest=dispose_xlsx_output_file,
                                                             con=con)
        print(f"Created: {dispose_xlsx_output_file}")

    con.close()

    # shutil.rmtree(path=working_dir)



def _exec_equi_delete(*,
                      worklist_path: str,
                      con: duckdb.DuckDBPyConnection) -> None:
    x03a_worklist_sql = duckdb_utils.create_landing_table_via_read(landing_table_name='s4_landing.equidelete_worklist',
                                                                           read_function='read_delete_worklist',
                                                                           source_file_path=worklist_path)
    
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_delete/01u_copy_databases.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_delete/02_create_macros.sql', con=con)
    con.execute(x03a_worklist_sql)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_delete/04_data_copy_to_eu_equi_change.sql', con=con)
    

