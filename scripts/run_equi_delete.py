# Expected to be called from a Makefile as arg list is complex


from argparse import ArgumentParser
import work_python2.equi_delete.equi_delete as equi_delete

def main(): 
    parser = ArgumentParser(description='Delete (dispose of) old equipment')
    parser.add_argument("--change_template_file", dest='change_template_file', required=True, help="Path to _create_ template file")
    parser.add_argument("--worklist", dest='worklist', required=True, help="Worklist file")
    parser.add_argument("--output_dispose_xlsx", dest='output_dispose_xlsx', help="Output file")
    parser.add_argument("--change_rec_name", dest='change_rec_name', required=False, help="Change request name [don't include number]")
    parser.add_argument("--change_rec_number", dest='change_rec_number', required=False, help="Change request number [don't include name]")
    args = parser.parse_args()
    change_template_file = args.change_template_file
    worklist = args.worklist
    output_dispose_xlsx = args.output_dispose_xlsx if args.output_dispose_xlsx else 'delete_equi.xlsx'
    change_rec_name = args.change_rec_name
    change_rec_number = args.change_rec_number


    equi_delete.run(eu_equi_change_template=change_template_file,
                    worklist_path=worklist, 
                    dispose_xlsx_output_file=output_dispose_xlsx,
                    change_rec_name=change_rec_name,
                    change_rec_number=change_rec_number)


main()
