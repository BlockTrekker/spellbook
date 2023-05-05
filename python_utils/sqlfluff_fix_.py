import os
import csv
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

def fix_sql_file(file_path):
    result = os.system(f'sqlfluff fix -f -p 32 --FIX-EVEN-UNPARSABLE {file_path}')
    if result != 0:
        lint_output = lint_sql_file(file_path)
        return file_path, lint_output
    return file_path, None

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
    updated_content = updated_content.replace('UNION\n', 'UNION ALL\n')

    with open(file_path, 'w') as f:
        f.write(updated_content)

def main():
    sql_files = []

    for root, dirs, files in os.walk('/home/outsider_analytics/Code/spellbook/models'):
        for file in files:
            if file.endswith('.sql'):
                file_path = os.path.join(root, file)
                sql_files.append(file_path)

    output_rows = []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(fix_sql_file, file_path): file_path for file_path in sql_files}
        for future in as_completed(futures):
            file_path, lint_output = future.result()
            if lint_output:
                unparsable_error = 'unparsable' in lint_output.lower()
                if 'CV06' in lint_output:
                    remove_semicolon(file_path)
                if unparsable_error:
                    wrap_alias_with_backticks(file_path)
                output_rows.append([file_path, lint_output])

    if output_rows:
        outputs_csv_path = '/home/outsider_analytics/Code/outputs.csv'
        with open(outputs_csv_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['File Path', 'Lint Output'])
            for row in output_rows:
                csvwriter.writerow(row)

if __name__ == "__main__":
    main()
