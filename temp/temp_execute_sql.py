


import tempfile
import os
import shutil
import duckdb
import work_python2.common.duckdb_utils as duckdb_utils

con = duckdb.connect(":default:")

duckdb_utils.execute_sql_file('data/test_script.sql', con=con)

print(con.fetchall())


setup1 = duckdb_utils.create_landing_table_via_read_union(landing_table_name='floc_delta_landing.worklist',
                                                          source_file_paths=['/home/stephen/_working/work/2025/floc_delta/bol10/BOL10_floc_delta_worklist.xlsx'],
                                                          read_function='read_floc_delta_worklist')

print(setup1)

# rel_file_path = "Scripts/asset_replace_gen/01u_attach_databases.sql"
# duckdb_utils.execute_work_sql_file(rel_file_path, con=con)

# con.execute("SELECT * FROM s4_classes_db.s4_classlists.vw_equi_class_defs LIMIT 3;")
# print(con.fetchall())

# con.close()

# td = tempfile.gettempdir()
# print(td)

# dirname = tempfile.mkdtemp(prefix='asset_replace_gen_')
# print(dirname)

# shutil.rmtree(path=dirname)

