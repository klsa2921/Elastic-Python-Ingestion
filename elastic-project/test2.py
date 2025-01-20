import csv

def load_empids_from_csv(file_path):
    empids = set()
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            empids.add(row['empid'])
    return empids

def compare_empids(file1, file2):
    empids_file1 = load_empids_from_csv(file1)
    empids_file2 = load_empids_from_csv(file2)

    uncommon_in_file1 = empids_file1 - empids_file2
    uncommon_in_file2 = empids_file2 - empids_file1

    return uncommon_in_file1, uncommon_in_file2

file1 = '/home/klsa/Documents/GitHub/Elastic-Python-Ingestion/elastic-project/new_data/employee.csv'
file2 = '/home/klsa/Documents/GitHub/Elastic-Python-Ingestion/elastic-project/new_data/salary.csv'

uncommon_in_file1, uncommon_in_file2 = compare_empids(file1, file2)

print(f"Uncommon empid in {file1}: {uncommon_in_file1}")
print(f"Uncommon empid in {file2}: {uncommon_in_file2}")