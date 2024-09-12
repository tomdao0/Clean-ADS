import pandas as pd
import os
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)


class MedicalDataProcessor:
    def __init__(self, folder_path, mapping_file):
        self.folder_path = folder_path
        self.mapping_file = mapping_file
        self.df_list = []

    def load_mapping(self):
        # Load the mapping file into a dictionary
        df_mapping = pd.read_excel(self.mapping_file, sheet_name="IP")
        df_mapping = df_mapping.dropna(how="all")
        return df_mapping.to_dict()

    def process_files(self):
        for root, dirs, files in os.walk(self.folder_path):
            for file in files:
                if file.endswith(".xlsx") and not file.startswith("~"):
                    file_path = os.path.join(root, file)

                    subfolder_level_1 = os.path.basename(root)

                    df = pd.read_excel(file_path)
                    df = df.dropna(how="all")
                    df["Source"] = subfolder_level_1
                    self.df_list.append(df)

    def apply_mapping(self, final_df, df_mapping):
        # Apply the mapping to the 'Insurance Name Plan1' column
        final_df["Insurance Name Plan1"] = final_df["Insurance Name Plan1"].astype(str)
        for i in range(len(df_mapping["ReplaceLowerContains"])):
            final_df["Insurance Name Plan1"] = final_df["Insurance Name Plan1"].apply(
                lambda x: (
                    df_mapping["ReplaceTo"][i]
                    if df_mapping["ReplaceLowerContains"][i] in x.lower()
                    else x
                )
            )
        return final_df

    def save_files(self, final_df):
        for source in final_df["Source"].unique():
            output_file = f"{source}.csv"
            final_df[final_df["Source"] == source].to_csv(output_file, index=False)

    def run(self):

        df_mapping = self.load_mapping()

        self.process_files()

        final_df = pd.concat(self.df_list, ignore_index=True)

        final_df = self.apply_mapping(final_df, df_mapping)

        self.save_files(final_df)


import pandas as pd
import os


class FeeScheduleProcessor:
    def __init__(self, folder_fee_schedule):
        self.folder_fee_schedule = folder_fee_schedule
        self.final_df = None

    def process_medicare(self):
        df_medicare = pd.read_excel(
            os.path.join(
                self.folder_fee_schedule, "Medicare FL loc 99 fee schedule.xlsx"
            ),
            usecols="A:F",
        )
        df_medicare = df_medicare.dropna(how="all")
        df_medicare = df_medicare[
            (df_medicare["NOTE"] != "#") & (df_medicare["PROCEDURE"].notna())
        ]
        df_medicare["PROCEDURE"] = df_medicare["PROCEDURE"].astype(str)
        df_medicare["MOD"] = df_medicare["MOD"].astype(str)
        df_medicare["MOD"].replace("nan", None, inplace=True)
        df_medicare = df_medicare.rename(columns={"PAR  AMOUNT": "Amount"})
        df_medicare["Key_FS"] = df_medicare[["PROCEDURE", "MOD"]].apply(
            lambda x: "".join([str(i) for i in x if pd.notnull(i)]), axis=1
        )
        df_medicare["Source"] = "Medicare FL"
        return df_medicare[["PROCEDURE", "Amount", "Key_FS", "Source"]]

    def process_aetna(self):
        df_aetna = pd.read_excel(
            os.path.join(self.folder_fee_schedule, "Aetna Fee schedule.xlsm"),
            usecols="B:H",
            skiprows=2,
            header=0,
        )
        df_aetna = df_aetna.dropna(how="all")
        df_aetna = df_aetna[(df_aetna["Customer/Network Name"].isna())]
        df_aetna["Procedure Code"] = df_aetna["Procedure Code"].astype(str)
        df_aetna["Modifier"] = df_aetna["Modifier"].astype(str)
        df_aetna["Modifier"].replace("nan", None, inplace=True)
        df_aetna["Key_FS"] = df_aetna[["Procedure Code", "Modifier"]].apply(
            lambda x: "".join([str(i) for i in x if pd.notnull(i)]), axis=1
        )
        df_aetna = df_aetna.rename(
            columns={"Procedure Code": "PROCEDURE", "Max Amount": "Amount"}
        )
        df_aetna["Source"] = "Aetna"
        return df_aetna[["PROCEDURE", "Amount", "Key_FS", "Source"]]

    def process_vaccn(self):
        df_vaccn = pd.read_excel(
            os.path.join(self.folder_fee_schedule, "VACCN fee schedule.xlsx"),
            usecols="A:I",
            skiprows=4,
            header=0,
        )
        df_vaccn = df_vaccn.dropna(how="all")
        df_vaccn["Procedure Code"] = df_vaccn["Procedure Code"].astype(str)
        df_vaccn["Modifier"] = df_vaccn["Modifier"].astype(str)
        df_vaccn = df_vaccn[df_vaccn["Locality Description"] == "REST OF FLORIDA"]
        df_vaccn["Modifier"].replace("N/A", None, inplace=True)
        df_vaccn["Modifier"].replace("nan", None, inplace=True)
        df_vaccn["Key_FS"] = df_vaccn[["Procedure Code", "Modifier"]].apply(
            lambda x: "".join([str(i) for i in x if pd.notnull(i)]), axis=1
        )
        df_vaccn = df_vaccn.rename(
            columns={"Procedure Code": "PROCEDURE", "Facility Rate": "Amount"}
        )
        df_vaccn["Source"] = "VACCN"
        return df_vaccn[["PROCEDURE", "Amount", "Key_FS", "Source"]]

    def process_capital(self):
        df_capital = pd.read_excel(
            os.path.join(self.folder_fee_schedule, "capital health plan.xlsx")
        )
        df_capital = df_capital.dropna(how="all")
        df_capital = df_capital[df_capital["NOTE"].isna()]
        df_capital["PROCEDURE"] = df_capital["PROCEDURE"].astype(str)
        df_capital["MOD"] = df_capital["MOD"].astype(str)
        df_capital["MOD"].replace("nan", None, inplace=True)
        df_capital["Key_FS"] = df_capital[["PROCEDURE", "MOD"]].apply(
            lambda x: "".join([str(i) for i in x if pd.notnull(i)]), axis=1
        )
        df_capital = df_capital.rename(columns={"PAR  AMOUNT": "Amount"})
        df_capital["Source"] = "Capital"
        return df_capital[["PROCEDURE", "Amount", "Key_FS", "Source"]]

    def combine_dataframes(self):
        df_aetna = self.process_aetna()
        df_capital = self.process_capital()
        df_medicare = self.process_medicare()
        df_vaccn = self.process_vaccn()
        dfs = [df_aetna, df_capital, df_medicare, df_vaccn]
        self.final_df = pd.concat(dfs, ignore_index=True)

    def save_to_csv(self, output_file):
        if self.final_df is not None:
            self.final_df.to_csv(output_file, index=False)
