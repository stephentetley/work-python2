# Expected to be called from a Makefile as arg list is complex

import os
from argparse import ArgumentParser
import work_python2.asset_replace.asset_replace as asset_replace

def main(): 
    parser = ArgumentParser(description='Generate new equipment')
    parser.add_argument("--create_template_file", dest='uploader_create_template', required=True, help="Path to _create_ template file")
    parser.add_argument("--change_template_file", dest='uploader_change_template', required=True, help="Path to _change_ template file")
    parser.add_argument("--worklist", dest='worklist', required=True, help="Worklist file")
    parser.add_argument("--ai2_masterdata_export", dest='ai2_masterdata_export', required=True, help="AI2 Masterdata export file")
    parser.add_argument("--ai2_eav_export", dest='ai2_eav_export', action="append", help="AI2 eavdata export file (can be >1)")
    parser.add_argument("--output_create_xlsx", dest='output_create_xlsx', help="Output file")
    args = parser.parse_args()
    uploader_create_template = args.uploader_create_template
    uploader_change_template = args.uploader_change_template
    worklist = args.worklist
    ai2_masterdata_export = args.ai2_masterdata_export
    ai2_eav_exports = args.ai2_eav_export if args.ai2_eav_export else []
    output_create_xlsx = args.output_create_xlsx if args.output_create_xlsx else 'create_equi.xlsx'

    asset_replace.run(worklist_path=worklist, 
                    ai2_masterdata_path=ai2_masterdata_export,
                    ai2_eav_exports=ai2_eav_exports,
                    eu_equi_create_template=uploader_create_template,
                    eu_equi_change_template=uploader_change_template,
                    create_xlsx_output_file=output_create_xlsx)


main()
