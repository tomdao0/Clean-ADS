import pandas as pd
import os
import warnings
import polars as pl
import re

warnings.simplefilter(action="ignore", category=FutureWarning)


class MedicalDataProcessor:
    def __init__(self, folder_path, mapping_file, output_folder):
        self.folder_path = folder_path
        self.mapping_file = mapping_file
        self.output_folder = output_folder
        self.df_list = []

    def load_mapping_IP(self):
        # Load the mapping file into a dictionary
        df_mapping = pd.read_excel(self.mapping_file, sheet_name="IP")
        df_mapping = df_mapping.dropna(how="all")
        return df_mapping.to_dict()

    def load_mapping_ICD(self):
        # Load the mapping file into a dictionary
        df_mapping_ICD = pd.read_excel(self.mapping_file, sheet_name="ICD")
        df_mapping_ICD = df_mapping_ICD.dropna(how="all")
        return df_mapping_ICD

    def get_ICD_code(self, txt):
        match = re.search(r"\[([^\]]+)\]", txt)
        if match:
            result = match.group(1)

        else:
            result = None
        return result

    def categorize_file(self, txt):
        if "medical" in txt.lower():
            result = "Medical"
        elif "psych" in txt.lower():
            result = "Psych"
        else:
            result = "NotFound"
        return result

    def process_files(self):
        for root, dirs, files in os.walk(self.folder_path):
            for file in files:
                if file.endswith(".xlsx") and not file.startswith("~"):
                    file_path = os.path.join(root, file)

                    subfolder_level_1 = os.path.basename(root)
                    temp = pl.read_excel(file_path)
                    df = temp.to_pandas()
                    # df = pd.read_excel(file_path)
                    df = df.dropna(how="all")
                    df["Source"] = subfolder_level_1 + "_" + self.categorize_file(file)
                    self.df_list.append(df)

    def apply_mapping(self, final_df, df_mapping_IP, df_mapping_ICD):
        # Apply the mapping to the 'Insurance Name Plan1' column
        final_df["Insurance Name Plan1"] = final_df["Insurance Name Plan1"].astype(str)
        for i in range(len(df_mapping_IP["ReplaceLowerContains"])):
            final_df["Insurance Name Plan1"] = final_df["Insurance Name Plan1"].apply(
                lambda x: (
                    df_mapping_IP["ReplaceTo"][i]
                    if df_mapping_IP["ReplaceLowerContains"][i] in x.lower()
                    else x
                )
            )
        for i in final_df.columns:
            if i[:3] == "Dia":
                final_df[i] = final_df[i].str.title().str.strip()
                for j in df_mapping_ICD.index:
                    final_df[i] = final_df[i].replace(
                        df_mapping_ICD.at[j, "Source"],
                        df_mapping_ICD.at[j, "Destination"],
                    )
        return final_df

    def save_files(self, final_df):
        for source in final_df["Source"].unique():
            output_file = f"{source}.csv"
            output_file = os.path.join(self.output_folder, output_file)
            final_df[final_df["Source"] == source].to_csv(output_file, index=False)

    def run(self):

        df_mapping_IP = self.load_mapping_IP()
        df_mapping_ICD = self.load_mapping_ICD()
        self.process_files()

        final_df = pd.concat(self.df_list, ignore_index=True)
        # Mapping CPT and IDC
        final_df = self.apply_mapping(final_df, df_mapping_IP, df_mapping_ICD)
        final_df["CPT Code"] = final_df["CPT Code"].astype(str)

        ICD_col = [col for col in final_df.columns if col[:3] == "Dia"]
        ICD_df = pd.concat([final_df[col] for col in ICD_col], axis=0).reset_index(
            drop=True
        )
        ICD_df = pd.DataFrame({"ICD": ICD_df})
        ICD_df = ICD_df[ICD_df["ICD"].notna()]
        ICD_df["Code"] = ICD_df["ICD"].apply(lambda x: self.get_ICD_code(x))
        self.save_files(final_df)
        ICD_df = ICD_df.drop_duplicates()
        # Filter for codes that have more than 1 ICD
        df_grouped = ICD_df.groupby("Code").ICD.nunique().reset_index()

        codes_with_multiple_icds = df_grouped[df_grouped["ICD"] > 1]["Code"]
        ICD_df = ICD_df[ICD_df["Code"].isin(codes_with_multiple_icds)]
        ICD_df.to_csv(
            os.path.join(self.output_folder, "Duplicate ICD.csv"), index=False
        )
        if ICD_df.shape[0] > 0:
            print(
                'Error Need to check Duplicate ICD.csv and then pull data mapping to sheet "ICD" of Mapping.xlsx'
            )
        s = "RJA, RPA, RTC, RWA, RYC, APC, CareSource, 49083, 92933, 94003, 99221, 99222, 99223, 99232, 99233, 99233C, 99233GC, 99233O, 99238, 99239, 99291, 99292, 99292C, S-101, S-102, S-105, S-107, MM-ERR, MM-NBE, MM-PNS, MM-PREV"
        items = [i.strip() for i in s.split(",")]
        CPT_df = pd.DataFrame(items, columns=["Code"])
        CPT_df["CategoryCPT"] = CPT_df["Code"]
        CPT_source_df = final_df["CPT Code"].drop_duplicates().dropna()
        missing_cpt_codes = CPT_source_df[~CPT_source_df.isin(CPT_df["Code"])]
        missing_cpt_df = pd.DataFrame(
            {"Code": missing_cpt_codes, "CategoryCPT": "Others"}
        )
        CPT_source = pd.concat([CPT_df, missing_cpt_df], ignore_index=True)
        CPT_source = CPT_source.astype(str)
        CPT_sorted = CPT_source[CPT_source["CategoryCPT"] != "Others"].sort_values(
            by="CategoryCPT"
        )
        CPT_sorted["index"] = range(1, len(CPT_sorted) + 1)

        others_index = len(CPT_sorted) + 1
        CPT_source.loc[CPT_source["CategoryCPT"] == "Others", "index"] = others_index

        CPT_final = pd.concat(
            [CPT_sorted, CPT_source[CPT_source["CategoryCPT"] == "Others"]],
            ignore_index=True,
        )
        CPT_final.to_csv(os.path.join(self.output_folder, "CPT.csv"), index=False)


class Billings:
    def __init__(self, folder_path, output_folder):
        self.folder_path = folder_path
        self.output_folder = output_folder
        self.df_list = []

    def categorize_file(self, txt):
        if "medical" in txt.lower():
            result = "Medical"
        elif "psych" in txt.lower():
            result = "Psych"
        else:
            result = "NotFound"
        return result

    def process_files(self):
        for root, dirs, files in os.walk(self.folder_path):
            for file in files:
                if file.endswith(".xlsx") and not file.startswith("~"):
                    file_path = os.path.join(root, file)

                    subfolder_level_1 = os.path.basename(root)
                    temp = pl.read_excel(file_path, sheet_name="Data")
                    df = temp.to_pandas()
                    # df = pd.read_excel(file_path)
                    df = df.dropna(how="all")
                    df["Source"] = subfolder_level_1 + "_" + self.categorize_file(file)
                    self.df_list.append(df)

    def save_files(self, final_df):
        for source in final_df["Source"].unique():
            output_file = f"{source}.csv"
            output_file = os.path.join(self.output_folder, output_file)
            final_df[final_df["Source"] == source].to_csv(output_file, index=False)

    def run(self):

        self.process_files()

        final_df = pd.concat(self.df_list, ignore_index=True)
        self.save_files(final_df)
