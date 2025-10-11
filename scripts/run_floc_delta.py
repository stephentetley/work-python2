
import os
import work_python2.floc_delta.floc_delta as floc_delta

uploader_create_template = os.path.expanduser(
    f'~/_working/work/2025/excel_uploader/templates/AIW_Floc_Creation_Template_V1.0.xlsx'
)

worklist  = os.path.expanduser(
    f'~/_working/work/2025/floc_delta/bol10/BOL10_floc_delta_worklist.xlsx'
)

ih06_files1 = [
    f'~/_working/work/2025/floc_delta/bol10/bol10_ih06_flocs_with_east_north.xlsx'
]

output_create_xlsx = os.path.expanduser(
    f'~/_working/work/2025/floc_delta/bol10/bol10_floc_create_upload2.xlsx'
)


ih06_files_expanded = [os.path.expanduser(path) for path in ih06_files1]

floc_delta.run(worklist_path=worklist, 
               ih06_exports=ih06_files_expanded,
               eu_floc_create_template=uploader_create_template, 
               xlsx_output_file=output_create_xlsx)

# con = duckdb.connect(":default:")

# execute_sql_script.execute_sql_file('data/test_script.sql', con=con)

# print(con.fetchall())


# rel_file_path = "Scripts/asset_replace_gen/01u_attach_databases.sql"
# execute_sql_script.execute_work_sql_file(rel_file_path, con=con)

# con.execute("SELECT * FROM s4_classes_db.s4_classlists.vw_equi_class_defs LIMIT 3;")
# print(con.fetchall())

# con.close()

# td = tempfile.gettempdir()
# print(td)

# dirname = tempfile.mkdtemp(prefix='asset_replace_gen_')
# print(dirname)

# shutil.rmtree(path=dirname)

