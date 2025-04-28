from scripts.helper import parse_dataframe

def download_kiva():
    kiva_dataset = ["kiva/data-science-for-good-kiva-crowdfunding",
             "kiva_loans.csv",
             ["id", "funded_amount", "loan_amount", "country_code", "currency", "activity", "sector", "date"],
             "kiva_loans.csv"]

    print(f"Downloading kiva dataset from Kaggle...")
    df = parse_dataframe(kiva_dataset[0], kiva_dataset[1], kiva_dataset[2])
    gcs_path = f"kiva/{kiva_dataset[3]}"
    print(f"uploading kiva dataset to GCS path: {gcs_path}...")

    return df, gcs_path