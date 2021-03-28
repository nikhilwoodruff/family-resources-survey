from family_resources_survey.save import save
from argparse import ArgumentParser

def main():
    parser = ArgumentParser(description="Utility to manage Family Resources Survey microdata.")
    parser.add_argument("action", choices=["save"], type=str, help="Save a UKDA FRS download.")
    parser.add_argument("--path", type=str, help="The path to the microdata download.")
    parser.add_argument("--year", choices=range(2010, 2020), type=int, help="The year of the FRS.")
    parser.add_argument("--zipped", action="store_true", help="Whether the download is zipped or a folder.")
    args = parser.parse_args()

    if args.action == "save":
        if args.path is None or args.year is None:
            print("A path and year must be provided.")
            exit(1)
        save(folder=args.path, year=args.year, zipped=args.zipped)