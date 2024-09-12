import pandas as pd
import os
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)


class MedicalDataProcessor:
    def __init__(self, folder_path, mapping_file, output_folder):
        self.folder_path = folder_path
        self.mapping_file = mapping_file
        self.output_folder = output_folder
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
            output_file = os.path.join(self.output_folder, output_file)
            final_df[final_df["Source"] == source].to_csv(output_file, index=False)

    def run(self):

        df_mapping = self.load_mapping()

        self.process_files()

        final_df = pd.concat(self.df_list, ignore_index=True)

        final_df = self.apply_mapping(final_df, df_mapping)

        self.save_files(final_df)
