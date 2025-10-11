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


def run(*, 
        worklist_path: str,
        ih06_exports: list[str]) -> None: 
    working_dir = tempfile.mkdtemp(prefix='asset_replace_gen_')
    duckdb_path = os.path.normpath(os.path.join(working_dir, 'floc_delta_db.duckdb'))

    con = duckdb.connect(database=duckdb_path)
    _exec_scripts(worklist_path=worklist_path, ih06_exports=ih06_exports, con=con)

    con.close()
    # shutil.rmtree(path=working_dir)
    print("Done")
    
def _exec_scripts(*, 
                  worklist_path: str,
                  ih06_exports: list[str],
                  con: duckdb.DuckDBPyConnection) -> None:
    x04a_landing_worklist_sql = duckdb_utils.create_landing_table_via_read(landing_table_name='floc_delta_landing.worklist',
                                                                           read_function='read_floc_delta_worklist',
                                                                           source_file_path=worklist_path)
    
    x04b_ih06_sql = duckdb_utils.create_landing_table_via_read_union(landing_table_name='floc_delta_landing.ih06_floc_exports',
                                                                           read_function='read_ih06_export',
                                                                           source_file_paths=ih06_exports)

    duckdb_utils.execute_work_sql_script(rel_path='Scripts/floc_delta/01u_copy_databases.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/floc_delta/02u_attach_databases.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/floc_delta/03_create_floc_delta_tables.sql', con=con)
    con.execute(x04a_landing_worklist_sql)
    con.execute(x04b_ih06_sql)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/floc_delta/05_floc_delta_insert_into.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/floc_delta/06_excel_uploader_insert_into.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/floc_delta/07_detach_databases.sql', con=con)
