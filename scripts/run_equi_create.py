# Expected to be called from a Makefile as arg list is complex


from argparse import ArgumentParser
import work_python2.equi_create.equi_create as equi_create

def main(): 
    parser = ArgumentParser(description='Generate new equipment')
    parser.add_argument("--create_template_file", dest='uploader_create_template', required=True, help="Path to _create_ template file")
    parser.add_argument("--worklist", dest='worklist', required=True, help="Worklist file")
    parser.add_argument("--ai2_masterdata_export", dest='ai2_masterdata_export', required=True, help="AI2 Masterdata export file")
    parser.add_argument("--ai2_eav_export", dest='ai2_eav_export', action="append", help="AI2 eavdata export file (can be >1)")
    parser.add_argument("--output_create_xlsx", dest='output_create_xlsx', help="Output file")
    parser.add_argument("--change_rec_name", dest='change_rec_name', required=False, help="Change request name")
    args = parser.parse_args()
    uploader_create_template = args.uploader_create_template
    worklist = args.worklist
    ai2_masterdata_export = args.ai2_masterdata_export
    ai2_eav_exports = args.ai2_eav_export if args.ai2_eav_export else []
    output_create_xlsx = args.output_create_xlsx if args.output_create_xlsx else 'create_equi.xlsx'
    change_rec_name = args.change_rec_name


    equi_create.run(eu_equi_create_template=uploader_create_template,
                    worklist_path=worklist, 
                    ai2_masterdata_path=ai2_masterdata_export,
                    ai2_eav_exports=ai2_eav_exports,
                    create_xlsx_output_file=output_create_xlsx,
                    change_rec_name=change_rec_name)


main()
