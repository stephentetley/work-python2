
# Expected to be called from a Makefile as arg list is complex


from argparse import ArgumentParser
import work_python2.floc_delta.floc_delta as floc_delta

def main(): 
    parser = ArgumentParser(description='Generate new flocs')
    parser.add_argument("--create_template_file", dest='uploader_create_template', required=True, help="Path to _create_ template file")
    parser.add_argument("--worklist", dest='worklist', required=True, help="Worklist file")
    parser.add_argument("--ih06_export", dest='ih06_export', action="append", help="IH06 export file (can be >1)")
    parser.add_argument("--ih06_glob", dest='ih06_glob', required=False, help="IH06 glob to find files")
    parser.add_argument("--output_create_xlsx", dest='output_create_xlsx', help="Output file")
    args = parser.parse_args()
    uploader_create_template = args.uploader_create_template
    worklist = args.worklist
    ih06_exports = args.ih06_export if args.ih06_export else []
    ih06_glob = args.ih06_glob
    output_create_xlsx = args.output_create_xlsx if args.output_create_xlsx else 'create_equi.xlsx'

    floc_delta.run(worklist_path=worklist,
                   ih06_exports=ih06_exports,
                   ih06_glob=ih06_glob,
                   eu_floc_create_template=uploader_create_template, 
                   xlsx_output_file=output_create_xlsx)

main()
