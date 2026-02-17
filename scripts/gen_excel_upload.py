from argparse import ArgumentParser
import re
import duckdb
import work_python2.common.excel_uploader_equi_change as excel_uploader_equi_change
import work_python2.common.excel_uploader_equi_create as excel_uploader_equi_create
import work_python2.common.excel_uploader_floc_create as excel_uploader_floc_create


def main(): 
    parser = ArgumentParser(description='Generate Excel Uploader equi change files')
    parser.add_argument("--uploader_template_file", dest='uploader_template_file', required=True, help="Path to Excel Uploader template file")
    parser.add_argument("--database_file", dest='database_file', required=True, help="Database file")
    parser.add_argument("--output_xlsx_file", dest='output_xlsx_file', required=True, help="Output file positional format string (applied to batch number)")
    parser.add_argument("--excel_uploader_output_type", dest='excel_uploader_output_type', required=True, help="Output type: floc_create, equi_create, equi_change")
    parser.add_argument("--title", dest='title', required=False, help="Change request title positional format string (applied to batch number and date.today())")
    args = parser.parse_args()
    uploader_template_file = args.uploader_template_file
    duckdb_path = args.database_file
    output_xlsx_file = args.output_xlsx_file
    excel_uploader_output_type = args.excel_uploader_output_type
    title = args.title if args.title else "Batch_{0:02} {1}"

    if not re.search(r"\{.*\}", output_xlsx_file):
        print("Adding batch suffix to the file names...")
        output_xlsx_file = output_xlsx_file.replace(".xlsx", "_batch{0}.xlsx") 


    con = duckdb.connect(database=duckdb_path, read_only=False)
    if excel_uploader_output_type.lower() == 'floc_create':
        excel_uploader_floc_create.insert_title_format_string(title_format=title, 
                                                              con=con)
        excel_uploader_floc_create.write_floc_create_uploads(upload_template_path=uploader_template_file,
                                                            dest=output_xlsx_file,
                                                            con=con)
    elif excel_uploader_output_type.lower() == 'equi_create':
        excel_uploader_equi_create.insert_title_format_string(title_format=title, 
                                                              con=con)
        excel_uploader_equi_create.write_equi_create_uploads(upload_template_path=uploader_template_file,
                                                            dest=output_xlsx_file,
                                                            con=con)
    elif excel_uploader_output_type.lower() == 'equi_change':
        excel_uploader_equi_change.insert_title_format_string(title_format=title, 
                                                              con=con)
        excel_uploader_equi_change.write_equi_change_uploads(upload_template_path=uploader_template_file,
                                                            dest=output_xlsx_file,
                                                            con=con)
    else: 
        print(f"Unrecognized excel_uploader_output_type: {excel_uploader_output_type}") 
    con.close()

    print("Done")

main()    

