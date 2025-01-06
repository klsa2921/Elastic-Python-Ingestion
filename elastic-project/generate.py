import csv
import random
from datetime import datetime, timedelta

# Sample employee IDs from your provided list
employee_ids = [
    'emp12345', 'emp12346', 'emp12347', 'emp12348', 'emp12349',
    'emp12350', 'emp12351', 'emp12352', 'emp12353', 'emp12354',
    'emp12355', 'emp12356', 'emp12357', 'emp12358', 'emp12359',
    'emp12360', 'emp12361', 'emp12362', 'emp12363', 'emp12364',
    'emp12365', 'emp12366', 'emp12367', 'emp12368', 'emp12369',
    'emp12370', 'emp12371', 'emp12372', 'emp12373', 'emp12374'
]

# Leave types and statuses
leave_types = ['sick', 'vacation', 'personal']
leave_statuses = ['approved', 'pending', 'denied']

# Function to generate random dates within a given range
def random_date(start_date, end_date):
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

# Function to create leave data for employees
def generate_leave_data(num_entries):
    leaves = []
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)

    for i in range(1, num_entries + 1):
        emp_id = random.choice(employee_ids)
        leave_type = random.choice(leave_types)
        leave_start_date = random_date(start_date, end_date).strftime('%Y-%m-%d')
        leave_end_date = (datetime.strptime(leave_start_date, '%Y-%m-%d') + timedelta(days=random.randint(1, 5))).strftime('%Y-%m-%d')
        leave_status = random.choice(leave_statuses)
        
        # Calculate number of days of leave
        start_date_obj = datetime.strptime(leave_start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(leave_end_date, '%Y-%m-%d')
        number_of_days = (end_date_obj - start_date_obj).days + 1  # Include both start and end day

        leave_entry = {
            'id': f'leave{i:03d}',
            'empid': emp_id,
            'leave_type': leave_type,
            'leave_start_date': leave_start_date,
            'number_of_days': number_of_days,
            'leave_end_date': leave_end_date,
            'leave_status': leave_status
        }
        
        leaves.append(leave_entry)
    
    return leaves

# Write the generated leave data to a CSV file
def write_to_csv(leaves, filename="./new_data/employee_leaves.csv"):
    fieldnames = ['id', 'empid', 'leave_type', 'leave_start_date', 'leave_end_date', 'number_of_days', 'leave_status']
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(leaves)

# Generate 200 leave entries (simplified to avoid large memory usage)
leave_data = generate_leave_data(200)

# Write the data to a CSV file
write_to_csv(leave_data)

print("CSV file 'employee_leaves.csv' has been generated.")
