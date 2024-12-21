import csv
import re

from config import CLEAN_PATTERN

input_file = 'queries_data_hiring_challenge_fast_simon.csv'
output_file = 'refined_cleaned_queries.csv'

with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
     open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    for row in reader:
        row[3] = re.sub(CLEAN_PATTERN, '', row[3])
        if len(row[3].replace("\"", "")) > 0:
            writer.writerow(row)
