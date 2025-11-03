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
import work_python2.common.excel_uploader_equi_create as excel_uploader_equi_create
import work_python2.common.excel_uploader_equi_change as excel_uploader_equi_change


def run(*, 
        worklist_path: str,
        ai2_masterdata_path: str,
        ai2_eav_exports: list[str],
        eu_equi_create_template: str,
        eu_equi_change_template: str,
        create_xlsx_output_file: str | None = None,
        change_xlsx_output_file: str | None = None) -> None: 
    working_dir = tempfile.mkdtemp(prefix='equi_create_')
    duckdb_gen_path = os.path.normpath(os.path.join(working_dir, 'equi_create_gen_db.duckdb'))
    con1 = duckdb.connect(database=duckdb_gen_path)
    
    _exec_equi_create_gen(worklist_path=worklist_path, 
                            ai2_masterdata_path=ai2_masterdata_path, 
                            con=con1)

    con1.close()
    
    duckdb_assetrep_path = os.path.normpath(os.path.join(working_dir, 'equi_create_db.duckdb'))
    con2 = duckdb.connect(database=duckdb_assetrep_path)

    _exec_equi_create(worklist_path=worklist_path, 
                        ai2_masterdata_path=ai2_masterdata_path,
                        ai2_eav_exports=ai2_eav_exports,                         
                        con=con2)

    if create_xlsx_output_file:
        excel_uploader_equi_create.write_equi_create_uploads(upload_template_path=eu_equi_create_template,
                                                             dest=create_xlsx_output_file,
                                                             con=con2)
        print(f"Created: {create_xlsx_output_file}")

    if change_xlsx_output_file:
        excel_uploader_equi_change.write_equi_change_uploads(upload_template_path=eu_equi_change_template,
                                                             dest=change_xlsx_output_file,
                                                             con=con2)
        print(f"Created: {change_xlsx_output_file}")

    con2.close()

    # shutil.rmtree(path=working_dir)



def _exec_equi_create_gen(*, 
                            worklist_path: str,
                            ai2_masterdata_path: str,
                            con: duckdb.DuckDBPyConnection) -> None:
    x03a_worklist_sql = duckdb_utils.create_landing_table_via_read(landing_table_name='equi_create_gen.s4_equipment',
                                                                           read_function='read_worklist_for_s4_classes',
                                                                           source_file_path=worklist_path)
    
    x03b_ai2_masterdata_sql = duckdb_utils.create_landing_table_via_read(landing_table_name='equi_create_gen.ai2_equipment ',
                                                                         read_function='read_ai2_masterdata_for_equipment',
                                                                         source_file_path=ai2_masterdata_path)

    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create_gen/01u_attach_databases.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create_gen/02_create_tables.sql', con=con)
    con.execute(x03a_worklist_sql)
    con.execute(x03b_ai2_masterdata_sql)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create_gen/04_gen_ai2_classrep_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create_gen/05_gen_s4_classrep_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create_gen/06_gen_ai2_eav_to_classrep_macros.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create_gen/07_gen_s4_classrep_insert_into.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create_gen/08_gen_s4_classrep_to_excel_uploader_chars.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create_gen/09u_copy_to_output_files.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create_gen/10_detach_databases.sql', con=con)


def _exec_equi_create(*, 
                        worklist_path: str,
                        ai2_masterdata_path: str,
                        ai2_eav_exports: list[str],
                        con: duckdb.DuckDBPyConnection) -> None:
    x11a_landing_worklist_sql = duckdb_utils.create_landing_table_via_read(landing_table_name='ai2_landing.worklist',
                                                                           read_function='read_equi_replace_worklist',
                                                                           source_file_path=worklist_path)
    
    x11b_ai2_masterdata_sql = duckdb_utils.create_landing_table_via_read(landing_table_name='ai2_landing.masterdata',
                                                                         read_function='read_ai2_export_masterdata',
                                                                         source_file_path=ai2_masterdata_path)

    eav_source_list = [ai2_masterdata_path] + ai2_eav_exports
    x11_ai2_eavdata_sql = duckdb_utils.create_landing_table_via_read_union(landing_table_name='ai2_landing.eavdata',
                                                                           read_function='read_ai2_export_eavdata',
                                                                           source_file_paths=eav_source_list
                                                                           )
    # TODO 01u and 02u have hard coded paths
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/01u_copy_databases.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/02u_attach_databases.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/03_create_translation_utility_macros.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/04_create_classrep_translation_macros.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/05_create_read_sources_macros.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/06_ai2_eav_create_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/07_ai2_classrep_create_master_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/output/08g_ai2_classrep_create_equiclass_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/09_s4_classrep_create_master_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/output/10g_s4_classrep_create_equiclass_tables.sql', con=con)
    con.execute(x11a_landing_worklist_sql)
    con.execute(x11b_ai2_masterdata_sql)
    con.execute(x11_ai2_eavdata_sql)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/12_data_copy_ai2_eav_to_classrep_masterdata.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/output/13g_data_copy_ai2_eav_to_classrep_equiclass.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/14_data_copy_ai2_to_s4_classrep_masterdata.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/output/15g_delete_from_s4_classrep_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/output/16g_insert_into_s4_classrep_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/17_data_copy_s4_classrep_to_eu_create_masterdata.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/output/18g_data_copy_s4_classrep_to_eu_create_eavdata.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/19_data_copy_worklist_to_eu_change.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/equi_create/20_detach_databases.sql', con=con)

