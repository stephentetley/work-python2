from argparse import ArgumentParser
import duckdb
import work_python2.common.excel_uploader_equi_change as excel_uploader_equi_change



def main(): 
    parser = ArgumentParser(description='Generate Excel Uploader equi change files')
    parser.add_argument("--change_template_file", dest='uploader_change_template', required=True, help="Path to _change_ template file")
    parser.add_argument("--database_file", dest='database_file', required=True, help="Database file")
    parser.add_argument("--output_change_xlsx", dest='output_change_xlsx', help="Output file")
    args = parser.parse_args()
    uploader_change_template = args.uploader_change_template
    duckdb_path = args.database_file
    output_change_xlsx = args.output_change_xlsx if args.output_change_xlsx else 'change_equi.xlsx'

    con = duckdb.connect(database=duckdb_path, read_only=False)
    excel_uploader_equi_change.write_equi_change_uploads(upload_template_path=uploader_change_template,
                                                        dest=output_change_xlsx,
                                                        con=con)

    con.close()

    print(f"Done")

main()    

