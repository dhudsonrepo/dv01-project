from helper import parse_dataframe, upload_to_gcs

KAGGLE_DATASETS = {
    "kiva": ["kiva/data-science-for-good-kiva-crowdfunding",
        "kiva_loans.csv",
         ["id", "funded_amount", "loan_amount", "country_code", "currency", "activity", "sector", "date"],
         "kiva_loans.csv"
    ],
    "lending_club": ["wordsforthewise/lending-club",
        "accepted_2007_to_2018q4.csv/accepted_2007_to_2018Q4.csv",
         ["id", "loan_amnt", "funded_amnt", "term", "int_rate", "grade", "sub_grade", "addr_state", "loan_status",
          "annual_inc", "dti"],
         "lending_club.csv"
    ]
}

for name, dataset in KAGGLE_DATASETS.items():
    print(f"Downloading {name} dataset from Kaggle...")
    df = parse_dataframe(dataset[0], dataset[1], dataset[2])
    gcs_path = f"{name}/{dataset[3]}"
    print(f"uploading {name} dataset to GCS path: {gcs_path}...")
    upload_to_gcs(df, gcs_path)