import pandas as pd
import prefect as pf

# @pf.task(name="[xlsx] export")
def export_toxlsx(df, file_name='./file.xlsx'):
    with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="overall", index=False)