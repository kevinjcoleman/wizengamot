import argparse
from main import Wizengamot

parser = argparse.ArgumentParser(
    description='This is Wizengamot, a program that magically sorts, compares and validates NationBuilder voter files.',
)

parser.add_argument('old_file', action="store", help="This is the dump file from the voter nation.")
parser.add_argument('new_file', action="store", help="This is the validated file from Trifacta.")
parser.add_argument('output_directory', action="store", help="This is where you want your files to be saved.")
args = parser.parse_args()
Wizengamot(args.old_file, args.new_file, args.output_directory)

# Invoke like
# spark-submit wizengamot.py old_file new_file output_directory
