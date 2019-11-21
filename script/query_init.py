#!/usr/bin/env python3
from argparse import ArgumentParser
import subprocess


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--dataset_id",
    help="Default dataset, if not specified all tables must be qualified with dataset",
)
parser.add_argument(
    "--project_id", help="Default project, if not specified the sdk will determine one"
)
parser.add_argument(
    "--destination_table", help="table where combined results will be written"
)
parser.add_argument("--date", help="date with dash, eg: 2019-10-24")


def main():
    args = parser.parse_args()
    # if "." not in args.dataset_id and args.project_id is not None:
    #     args.dataset_id = f"{args.project_id}.{args.dataset_id}"
    # if "." not in args.destination_table and args.dataset_id is not None:
    #     args.destination_table = f"{args.dataset_id}.{args.destination_table}"

    sql_file_path = None
    parameters = []
    arguments = ("--use_legacy_sql=false", "--replace", "--max_rows=0")
    date_partition_parameter = "submission_date"

    project_id = args.project_id
    dataset_id = args.dataset_id
    destination_table = None # using DDL statement, no destination needed

    sql_file_path = sql_file_path or "sql/{}/{}/init.sql".format(
        dataset_id, args.destination_table
    )
    if date_partition_parameter is not None:
        parameters += (date_partition_parameter + ":DATE:" + args.date,)

    arguments = (
        ["query"]
        + (["--destination_table=" + destination_table] if destination_table else [])
        + ["--dataset_id=" + dataset_id]
        + (["--project_id=" + project_id] if project_id else [])
        + ["--parameter=" + parameter for parameter in parameters]
        + list(arguments)
        + ["<", sql_file_path]
    )

    subprocess.check_call(["bash", "-c", " ".join(["bq"] + arguments)])


if __name__ == "__main__":
    main()
