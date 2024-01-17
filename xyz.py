# import csv
#
# # Input CSV file
# input_file = "XYZ.csv"
#
# # Output CSV file without duplicates
# output_file = "output_no_duplicates.csv"
#
# # Create a set to keep track of seen names
# seen_names = set()
#
# # Open the input and output CSV files
# with open(input_file, 'r', newline='') as in_csvfile, open(output_file, 'w', newline='') as out_csvfile:
#     reader = csv.reader(in_csvfile)
#     writer = csv.writer(out_csvfile)
#
#     # Write the header
#     header = next(reader)
#     writer.writerow(header)
#
#     # Iterate through the rows
#     for row in reader:
#         name = row[0]  # Assuming the name is in the first column
#
#         # Check if the name has been seen before
#         if name not in seen_names:
#             # If it's a new name, write the row to the output file and mark it as seen
#             writer.writerow(row)
#             seen_names.add(name)
#
# print("Duplicates removed. Output saved to", output_file)





#
# import requests
# from bs4 import BeautifulSoup
#
# url = 'https://iclr.cc/Conferences/2019/ProgramCommittee'
#
# # Fetch the HTML content of the website
# response = requests.get(url)
# html_content = response.text
#
# # Parse the HTML content with BeautifulSoup
# soup = BeautifulSoup(html_content, 'html.parser')
#
# # Find all the review blocks
# review_blocks = soup.find_all('div', class_='reviewer-block')
#
# # Extract names from each review block
# all_names = []
# for review_block in review_blocks:
#     names = review_block.get_text(separator='\n').strip().split('\n')
#     all_names.extend(names)
#
# # Print the extracted names
# # for idx, name in enumerate(all_names, start=1):
# #     print(f"{idx}. {name}")
#
# all_names = [name.strip() for review_block in review_blocks for name in review_block.get_text(separator='\n').strip().split('\n') if name.strip()]
# import csv
#
# # ...
#
# # Create a list of dictionaries for each person with 'affiliation' set to 'Unknown'
# people_data = [{'name': name, 'affiliation': 'Unknown'} for name in all_names]
#
# # Specify the existing CSV file name
# csv_file_name = 'people_data.csv'
#
# # Write the list of dictionaries to the existing CSV file in append mode
# with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
#     fieldnames = ['name', 'affiliation']
#     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#
#     # If the file is empty, write the header
#     if csvfile.tell() == 0:
#         writer.writeheader()
#
#     # Write the data
#     for person_data in people_data:
#         writer.writerow(person_data)
#
# print(f"Data has been appended to {csv_file_name}")


import requests
from bs4 import BeautifulSoup

# URL of the website
url = 'https://www.auai.org/uai2021/program_committee'

# Fetch the HTML content from the website
response = requests.get(url)
html_content = response.text

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(html_content, 'html.parser')

# Find the section with the list of Program Committee Members
program_committee_section = soup.find('h2', text='Full List of Program Committee Members')

# Extract names and affiliations
names_and_affiliations = []
for item in program_committee_section.find_next('script').stripped_strings:
    names_and_affiliations.extend(item.split(', &nbsp;'))

# Remove any leading or trailing whitespaces
names_and_affiliations = [name.strip() for name in names_and_affiliations]

# Filter out empty strings and create a list of dictionaries
people_data = [{'name': name, 'affiliation': 'Unknown'} for name in names_and_affiliations if name]

# Print the list of dictionaries
for idx, person_data in enumerate(people_data, start=1):
    print(f"{idx}. {person_data}")
