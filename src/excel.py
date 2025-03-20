from openpyxl.utils.dataframe import dataframe_to_rows
 
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
 