import os
import duckdb
import work_python2.common.excel_uploader_equi_change as excel_uploader_equi_change


duckdb_path = os.path.expanduser(f'~/_working/work/2026/data_cleansing/duckdb/manuf_recovery_db.duckdb')
uploader_change_template = os.path.expanduser('~/_working/work/2025/excel_uploader/templates/AIW_Equi_Change_Template_V1.0.xlsx')
output_change_xlsx = os.path.expanduser(f'~/_working/work/2026/data_cleansing/duckdb/manuf_recovery_CHANGE.xlsx')



con = duckdb.connect(database=duckdb_path, read_only=False)
excel_uploader_equi_change.write_equi_change_uploads(upload_template_path=uploader_change_template,
                                                    dest=output_change_xlsx,
                                                    con=con)

con.close()

print(f"Done")

