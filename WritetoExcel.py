import pandas as pd
import atlas_pull as ap
import xlwings as xw

def Execute():
    # Create a sample DataFrame
    under_performers, top_performers = ap.atlas_update()

    # Rename 'aaLogin' to 'User ID' in both DataFrames
    top_performers = top_performers.rename(columns={'aaLogin': 'User ID'})
    under_performers = under_performers.rename(columns={'aaLogin': 'User ID'})

    # Open the existing Excel workbook
    wb = xw.Book('Weekly DPMO Tracker.xlsm')

    # Write DataFrames to Excel sheets
    wb.sheets['Bottom Performers'].range('A1').options(index=False).value = under_performers
    wb.sheets['Top Performers'].range('A1').options(index=False).value = top_performers

    # Get the worksheet objects
    top_sheet = wb.sheets['Top Performers']
    under_sheet = wb.sheets['Bottom Performers']
    roster_sheet = wb.sheets['Roster']
    filtered_top_sheet = wb.sheets['AFE -> Sort Scrub']
    filtered_bottom_sheet = wb.sheets['Sort -> AFE Scrub']

    # Check if the sheet exists, if not, create it
    if 'Sort -> AFE Scrub' not in [sheet.name for sheet in wb.sheets]:
        filtered_bottom_sheet = wb.sheets.add('Sort -> AFE Scrub', after='Sort -> AFE Scrub')
    else:
        filtered_bottom_sheet = wb.sheets['Sort -> AFE Scrub']

    # Clear existing tables if any
    for sheet in [top_sheet, under_sheet, filtered_top_sheet, filtered_bottom_sheet]:
        for table in sheet.tables:
            table.range.delete()


    # Add tables with formatting
    top_sheet.tables.add(top_sheet.range('A1').expand(), name='TopPerformers', table_style_name='TableStyleMedium7')
    under_sheet.tables.add(under_sheet.range('A1').expand(), name='UnderPerformers', table_style_name='TableStyleMedium10')

    # Read Roster sheet into a DataFrame
    roster_data = roster_sheet.range('A1').expand().options(pd.DataFrame, header=1, index=False).value

    # Create two separate filtered roster DataFrames
    filtered_roster_16 = roster_data[
        (roster_data['Management Area ID'] == 16) & 
        ((roster_data['Shift Pattern'].str.startswith('D')) | (roster_data['Shift Pattern'] == 'X6S-0730'))
    ]

    filtered_roster_14 = roster_data[
        (roster_data['Management Area ID'] == 14) & 
        ((roster_data['Shift Pattern'].str.startswith('D')) | (roster_data['Shift Pattern'] == 'X6S-0730'))
    ]

    # Merge filtered roster with top performers using User ID for Area ID 16
    merged_top_performers_16 = pd.merge(
        top_performers, 
        filtered_roster_16[['User ID', 'Management Area ID', 'Job Title', 'Shift Pattern']], 
        on='User ID', 
        how='inner'
    )

    # Merge filtered roster with bottom performers using User ID for Area ID 14
    merged_bottom_performers_14 = pd.merge(
        under_performers, 
        filtered_roster_14[['User ID', 'Management Area ID', 'Job Title', 'Shift Pattern']], 
        on='User ID', 
        how='inner'
    )

    # Create sheets for filtered performers
    filtered_top_sheet = wb.sheets['AFE -> Sort Scrub']
    filtered_bottom_sheet = wb.sheets['Sort -> AFE Scrub']

    # Write merged data to the sheets
    filtered_top_sheet.range('A1').options(index=False).value = merged_top_performers_16
    filtered_bottom_sheet.range('A1').options(index=False).value = merged_bottom_performers_14

    # Add tables with formatting to the sheets
    filtered_top_sheet.tables.add(
        filtered_top_sheet.range('A1').expand(), 
        name='FilteredTopPerformers16', 
        table_style_name='TableStyleMedium13'
    )

    filtered_bottom_sheet.tables.add(
        filtered_bottom_sheet.range('A1').expand(), 
        name='FilteredBottomPerformers14', 
        table_style_name='TableStyleMedium10'
    )

    # Save the workbook
    wb.save('Weekly DPMO Tracker.xlsm')
    wb.close()

if __name__ == "__main__":
    Execute()
