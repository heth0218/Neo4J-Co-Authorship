# Open the file in read mode
with open('new_data.txt', 'r') as file:
    # Read the content of the file
    content = file.read()

    # Split the content into a list using commas as separators
    names_list = content.split(', ')
    print(len(names_list))