from scripts.helper import parse_dataframe

def lending_club_download():
    lending_dataset = ["wordsforthewise/lending-club",
        "accepted_2007_to_2018q4.csv/accepted_2007_to_2018Q4.csv",
        ["id", "loan_amnt", "funded_amnt", "term", "int_rate", "grade", "sub_grade", "addr_state", "loan_status",
        "issue_d", "annual_inc", "dti"],
        "lending_club.csv"
    ]

    print(f"Downloading lending_club dataset from Kaggle...")
    df = parse_dataframe(lending_dataset[0], lending_dataset[1], lending_dataset[2])
    gcs_path = f"lending_club/{lending_dataset[3]}"
    print(f"uploading lending_club dataset to GCS path: {gcs_path}...")

    return df, gcs_path