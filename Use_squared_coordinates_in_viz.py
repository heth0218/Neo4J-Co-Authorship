import pandas as pd

# Load nlp_map.csv and nlp_squared_map.csv into DataFrames
nlp_map = pd.read_csv('output/nlp_map.csv')
nlp_squared_map = pd.read_csv('output/nlp_squared_map.csv')

# Iterate through each row in nlp_map
for index, row in nlp_map.iterrows():
    # Get the id value from the current row
    current_id = row['id']

    # Find the corresponding row in nlp_squared_map
    squared_row = nlp_squared_map[nlp_squared_map['id'] == current_id]

    # Check if a corresponding row is found
    if not squared_row.empty:
        # Get x and y values from nlp_squared_map
        new_x = squared_row['x'].values[0]
        new_y = squared_row['y'].values[0]

        # Update the x and y values in nlp_map
        nlp_map.at[index, 'x'] = new_x
        nlp_map.at[index, 'y'] = new_y

# Save the updated nlp_map DataFrame to a new CSV file
nlp_map.to_csv('updated_nlp_map.csv', index=False)
