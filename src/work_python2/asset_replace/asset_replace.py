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
import work_python2.common.excel_uploader_floc_create as excel_uploader_floc_create


def run(*, 
        worklist_path: str,
        ai2_masterdata_path: str,
        ai2_eav_exports: list[str],
        eu_equi_create_template: str,
        eu_equi_change_template: str,
        create_xlsx_output_file: str | None = None,
        change_xlsx_output_file: str | None = None) -> None: 
    working_dir = tempfile.mkdtemp(prefix='asset_replace_gen_')
    duckdb_gen_path = os.path.normpath(os.path.join(working_dir, 'asset_replace_gen_db.duckdb'))
    con1 = duckdb.connect(database=duckdb_gen_path)
    
    _exec_asset_replace_gen(worklist_path=worklist_path, 
                            ai2_masterdata_path=ai2_masterdata_path, 
                            con=con1)

    con1.close()
    
    duckdb_assetrep_path = os.path.normpath(os.path.join(working_dir, 'asset_replace_db.duckdb'))
    con2 = duckdb.connect(database=duckdb_assetrep_path)

    _exec_asset_replace(worklist_path=worklist_path, 
                        ai2_masterdata_path=ai2_masterdata_path,
                        ai2_eav_exports=ai2_eav_exports,                         
                        con=con2)

    if create_xlsx_output_file:
        excel_uploader_floc_create.write_excel_upload(upload_template_path=eu_equi_create_template,
                                                      dest=create_xlsx_output_file,
                                                      con=con2)
        print(f"Created: {create_xlsx_output_file}")

    if change_xlsx_output_file:
        excel_uploader_floc_create.write_excel_upload(upload_template_path=eu_equi_change_template,
                                                      dest=change_xlsx_output_file,
                                                      con=con2)
        print(f"Created: {change_xlsx_output_file}")

    con2.close()

    # shutil.rmtree(path=working_dir)



def _exec_asset_replace_gen(*, 
                            worklist_path: str,
                            ai2_masterdata_path: str,
                            con: duckdb.DuckDBPyConnection) -> None:
    x03a_worklist_sql = duckdb_utils.create_landing_table_via_read(landing_table_name='asset_replace_gen.s4_equipment',
                                                                           read_function='read_worklist_for_s4_classes',
                                                                           source_file_path=worklist_path)
    
    x03b_ai2_masterdata_sql = duckdb_utils.create_landing_table_via_read(landing_table_name='asset_replace_gen.ai2_equipment ',
                                                                         read_function='read_ai2_masterdata_for_equipment',
                                                                         source_file_path=ai2_masterdata_path)

    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace_gen/01u_attach_databases.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace_gen/02_create_tables.sql', con=con)
    con.execute(x03a_worklist_sql)
    con.execute(x03b_ai2_masterdata_sql)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace_gen/04_gen_ai2_classrep_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace_gen/05_gen_s4_classrep_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace_gen/06_gen_ai2_eav_to_classrep_macros.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace_gen/07_gen_s4_classrep_insert_into.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace_gen/08_gen_s4_classrep_to_excel_uploader_chars.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace_gen/09u_copy_to_output_files.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace_gen/10_detach_databases.sql', con=con)


def _exec_asset_replace(*, 
                        worklist_path: str,
                        ai2_eav_exports: list[str],
                        ai2_masterdata_path: str,
                        con: duckdb.DuckDBPyConnection) -> None:
    # x03a_landing_worklist_sql = duckdb_utils.create_landing_table_via_read(landing_table_name='floc_delta_landing.worklist',
    #                                                                        read_function='read_floc_delta_worklist',
    #                                                                        source_file_path=worklist_path)
    
    # x03b_ai2_masterdata_sql = duckdb_utils.create_landing_table_via_read(landing_table_name='floc_delta_landing.ih06_floc_exports',
    #                                                                      read_function='read_ih06_export',
    #                                                                      source_file_path=ai2_masterdata_path)

    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/01u_copy_databases.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/02u_attach_databases.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/03_create_translation_utility_macros.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/04_create_classrep_translation_macros.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/05_create_read_sources_macros.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/06_ai2_eav_create_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/07_ai2_classrep_create_master_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/08x_ai2_classrep_create_equiclass_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/09_s4_classrep_create_master_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/10x_s4_classrep_create_equiclass_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/11u_data_import_to_landing.sql', con=con)

    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/12_data_copy_ai2_eav_to_classrep_masterdata.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/13x_data_copy_ai2_eav_to_classrep_equiclass.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/14_data_copy_ai2_to_s4_classrep_masterdata.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/15x_delete_from_s4_classrep_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/16x_insert_into_s4_classrep_tables.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/17_data_copy_s4_classrep_to_eu_create_masterdata.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/18x_data_copy_s4_classrep_to_eu_create_eavdata.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/19_data_copy_worklist_to_eu_change.sql', con=con)
    duckdb_utils.execute_work_sql_script(rel_path='Scripts/asset_replace/20_detach_databases.sql', con=con)

