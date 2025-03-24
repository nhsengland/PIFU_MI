from openpyxl.utils.dataframe import dataframe_to_rows
from copy import copy

def insert_pandas_df_into_excel(df, ws, header=True, startrow=1, startcol=1, index=True):
    """
    Inserts a pandas dataframe into an excel worksheet
    Parameters:
    df: (pandas DataFrame): The pandas dataframe to be inserted
    ws: (openpyxl sheet object): The openpyxl sheet object to insert the dataframe into (e.g. sheets['Data'])
    startrow: (int): The starting row to insert the dataframe (default 0)
    startcol: (int): The starting column to insert the dataframe (default 0)
    index: (bool): Whether to include the index column in the dataframe (default True)
    """
    rows = dataframe_to_rows(df, header=header, index=index)

    for r_idx, row in enumerate(rows, startrow):
        for c_idx, value in enumerate(row, startcol):
             ws.cell(row=r_idx, column=c_idx).value = value

             

def copy_all_cell_styles(from_cell, to_cell):
    """
    Copies all cell styles from one cell to another.

    Parameters:
    from_cell: (openpyxl cell object): The cell from which to copy the styles.
    to_cell: (openpyxl cell object): The cell to which the styles will be copied.

    Returns:
    to_cell: (openpyxl cell object): The cell with the copied styles.
    """
    to_cell.font = copy(from_cell.font)
    to_cell.border = copy(from_cell.border)
    to_cell.fill = copy(from_cell.fill)
    to_cell.number_format = copy(from_cell.number_format)
    to_cell.protection = copy(from_cell.protection)
    to_cell.alignment = copy(from_cell.alignment)

    return to_cell
 