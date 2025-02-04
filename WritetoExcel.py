import pandas as pd
import atlas_pull as ap
import xlsxwriter

def Execute():# Create a sample DataFrame
    under_performers, top_performers = ap.atlas_update()
    # Create an Excel writer object
    writer = pd.ExcelWriter('Sort_DPMO.xlsx', engine='xlsxwriter')
    # Write the DataFrame to Excel
    under_performers.to_excel(writer, sheet_name='Lower', index=False)
    top_performers.to_excel(writer, sheet_name='Top', index=False)
    # Get the workbook and worksheet objects
    workbook = writer.book
    top_sheet = writer.sheets['Top']
    under_sheet = writer.sheetS['Lower']

    # Add a table with formatting
    #for the underperforming AAs

    top_sheet.add_table('A1:B3', {'style': 'Table Style Medium 10'})

    #for the AAs who are top performers
    # Add a table with formatting
    #for the underperforming AAs
    under_sheet.add_table('A1:B3', {'style': 'Table Style Medium 7'})

    #for the AAs who can be roster scrubbed
    # Add a table with formatting
    #for the underperforming AAs
    under_sheet.add_table('A1:B3', {'style': 'Table Style Medium 13'})
    # Close the writer object
    writer.save()

if __name__ == "__main__":
    Execute()