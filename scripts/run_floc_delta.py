
import os
import work_python2.floc_delta.floc_delta as floc_delta

uploader_create_template = os.path.expanduser(
    f'~/_working/work/2025/excel_uploader/templates/AIW_Floc_Creation_Template_V1.0.xlsx'
)

worklist  = os.path.expanduser(
    f'~/_working/work/2025/capital_schemes/hai11/flocs/hai11_floc_delta_worklist.xlsx'
)

ih06_files1 = [
    f'~/_working/work/2025/capital_schemes/hai11/flocs/hai11_ih06_with_east_north.xlsx'
]

output_create_xlsx = os.path.expanduser(
    f'~/_working/work/2025/capital_schemes/hai11/flocs/hai11_floc_create_NEW.xlsx'
)


ih06_files_expanded = [os.path.expanduser(path) for path in ih06_files1]

floc_delta.run(worklist_path=worklist, 
               ih06_exports=ih06_files_expanded,
               eu_floc_create_template=uploader_create_template, 
               xlsx_output_file=output_create_xlsx)
