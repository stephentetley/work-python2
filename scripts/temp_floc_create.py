import os
import duckdb
import work_python2.common.excel_uploader_floc_create as excel_uploader_floc_create


duckdb_path = os.path.expanduser(f'~/_working/work/2026/metrel/metrel_floc_delta_db.duckdb')
uploader_create_template = os.path.expanduser(
    '~/_working/work/resources/uploader_templates/AIW_Floc_Creation_Template_V1.0.xlsx'
)
output_change_xlsx = os.path.expanduser(f'~/_working/work/2026/metrel/elf01_flocs_CREATE.xlsx')



con = duckdb.connect(database=duckdb_path, read_only=False)
excel_uploader_floc_create.write_equi_create_uploads(upload_template_path=uploader_create_template,
                                                     dest=output_change_xlsx,
                                                     con=con)

con.close()

print(f"Done")

