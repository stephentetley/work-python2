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
import xlsxwriter
import work_python2.common.duckdb_utils as duckdb_utils
import work_python2.common.excel_utils as excel_utils



def run(*, 
        s4_ih08_exports: list[str],
        ai2_equi_exports: list[str],
        output_xlsx_path: str) -> None: 
    working_dir = tempfile.mkdtemp(prefix='equi_compare_')
    duckdb_gen_path = os.path.normpath(os.path.join(working_dir, 'equi_compare_db.duckdb'))
    con = duckdb.connect(database=duckdb_gen_path)
    
    _exec_equi_compare(s4_ih08_exports=s4_ih08_exports,
                       ai2_equi_exports=ai2_equi_exports, 
                       con=con)

    query = f"""
        SELECT 
            * 
        FROM equi_compare.output_report
        ORDER BY site_name, floc;
    """
    if output_xlsx_path:
        with xlsxwriter.Workbook(output_xlsx_path) as workbook:
            excel_utils.write_sql_query_to_excel(select_query=query,
                                                 sheet_name='Sheet1',
                                                 con=con,
                                                 workbook=workbook)
        print(f"Created: {output_xlsx_path}")

    con.close()

    # shutil.rmtree(path=working_dir)



def _exec_equi_compare(*, 
                       s4_ih08_exports: list[str],
                       ai2_equi_exports: list[str],
                       con: duckdb.DuckDBPyConnection) -> None:
    x04a_s4_masterdata_load = duckdb_utils.create_landing_table_via_read_union(landing_table_name='equi_compare_landing.ih08_equi_masterdata',
                                                                         read_function='read_ih08_export',
                                                                         source_file_paths=s4_ih08_exports)
    
    x04b_ai2_masterdata_load = duckdb_utils.create_landing_table_via_read_union(landing_table_name='equi_compare_landing.ai2_equi_masterdata',
                                                                               read_function='read_ai2_equi_report',
                                                                               source_file_paths=ai2_equi_exports)

    # TODO 03u contains a hardcoded path
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_compare/01_create_compare_status_type.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_compare/02_setup_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_compare/03u_import_equi_factdata.sql', con=con)
    con.execute(x04a_s4_masterdata_load)
    con.execute(x04b_ai2_masterdata_load)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_compare/05_create_report_source_table.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_compare/06_create_report_output_table.sql', con=con)


