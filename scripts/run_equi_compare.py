
import os
import work_python2.equi_compare.equi_compare as equi_compare

s4_ih08_exports = [
    f'~/_working/work/2025/equi_compare/lstnut_oct25/lstnut_ih08_with_aib_ref.xlsx'
]

ai2_equi_exports = [
    f'~/_working/work/2025/equi_compare/lstnut_oct25/AI2UltrasonicLevelInstruments_20251020.xlsx'
]

output_create_xlsx = os.path.expanduser(
    f'~/_working/work/2025/equi_compare/lstnut_oct25/equi_compare_lstnut.xlsx'
)

s4_ih08_exports_expanded = [os.path.expanduser(path) for path in s4_ih08_exports]
ai2_equi_exports_expanded = [os.path.expanduser(path) for path in ai2_equi_exports]

equi_compare.run(s4_ih08_exports=s4_ih08_exports_expanded,
                 ai2_equi_exports=ai2_equi_exports_expanded,
                 output_xlsx_path=output_create_xlsx)



