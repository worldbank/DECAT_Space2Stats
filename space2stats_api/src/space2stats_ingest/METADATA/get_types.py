import json
import os
from os.path import join

import git
import pandas as pd


# Function to get the root of the git repository
def get_git_root() -> str:
    git_repo = git.Repo(os.getcwd(), search_parent_directories=True)
    return git_repo.git.rev_parse("--show-toplevel")


# Function to load a subset of the Parquet data
def load_parquet_data_subset(parquet_file: str, num_rows: int) -> pd.DataFrame:
    # Load only a specific number of rows from the Parquet file
    df = pd.read_parquet(parquet_file)
    return df.head(num_rows)


def save_parquet_types_to_json(parquet_file: str, json_file: str):
    df = pd.read_parquet(parquet_file)
    # Get the column names and their types
    column_types = {col: str(df[col].dtype) for col in df.columns}

    # Save the column types to a JSON file
    with open(json_file, "w") as f:
        json.dump(column_types, f, indent=4)

    print(f"Column types saved to {json_file}")


if __name__ == "__main__":
    git_root = get_git_root()
    parquet_file = join(git_root, "space2stats_api/src/space2stats.parquet")
    json_file = join(
        git_root, "space2stats_api/src/space2stats_ingest/METADATA/types.json"
    )

    # Ensure the directory exists
    os.makedirs(os.path.dirname(json_file), exist_ok=True)

    # Save the Parquet column types to JSON
    save_parquet_types_to_json(parquet_file, json_file)
