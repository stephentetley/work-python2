
import os
import work_python2.asset_replace.asset_replace as asset_replace

uploader_create_template = os.path.expanduser(
    f'~/_working/work/2025/excel_uploader/templates/AIW_Equi_Creation_Template_V1.0.xlsx'
)

uploader_change_template = os.path.expanduser(
    f'~/_working/work/2025/excel_uploader/templates/AIW_Equi_Change_Template_V1.0.xlsx'
)

worklist = os.path.expanduser(
    f'~/_working/work/2025/capital_schemes/hai11/equi/hai11_equi_assetrep_worklist.xlsx'
)

ai2_masterdata_export = os.path.expanduser(
    f'~/_working/work/2025/capital_schemes/hai11/equi/hai11_ai2_equi_masterdata_export.xlsx'
)

ai2_eav_exports = [
    # f'~/_working/work/2025/capital_schemes/hai11/equi/hai11_ai2_valv_eavdata.xlsx'

]

output_create_xlsx = os.path.expanduser(
    f'~/_working/work/2025/capital_schemes/hai11/equi/hai11_equi_create_NEW.xlsx'
)


ai2_eav_files_expanded = [os.path.expanduser(path) for path in ai2_eav_exports]

asset_replace.run(worklist_path=worklist, 
                  ai2_masterdata_path=ai2_masterdata_export,
                  ai2_eav_exports=ai2_eav_exports,
                  eu_equi_create_template=uploader_create_template,
                  eu_equi_change_template=uploader_change_template,
                  create_xlsx_output_file=output_create_xlsx)



