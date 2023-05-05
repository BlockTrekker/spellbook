import csv
import os
import re
import subprocess

def lint_sql_file(file_path):
    result = subprocess.run(['sqlfluff', 'lint', file_path], capture_output=True, text=True)
    return result.stdout

def remove_semicolon(file_path):
    with open(file_path, 'rb+') as f:
        f.seek(-1, os.SEEK_END)
        if f.read(1) == b';':
            f.seek(-1, os.SEEK_END)
            f.truncate()

def wrap_alias_with_backticks(file_path):
    with open(file_path, 'r') as f:
        content = f.read()

    updated_content = re.sub(r'(?<=\s)AS\s+(\w+)(?=\s)', r'AS `\1`', content, flags=re.IGNORECASE)

    with open(file_path, 'w') as f:
        f.write(updated_content)

def main():
    csv_file_path = '/home/outsider_analytics/Code/unfixable.csv'

    with open(csv_file_path, 'r', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            file_path = row[0]
            lint_output = lint_sql_file(file_path)
            if 'CV06' in lint_output:
                remove_semicolon(file_path)
                print(f'Semicolon removed from {file_path}')
            if 'L029' in lint_output:
                wrap_alias_with_backticks(file_path)
                print(f'Wrapped alias with backticks in {file_path}')

if __name__ == "__main__":
    main()






