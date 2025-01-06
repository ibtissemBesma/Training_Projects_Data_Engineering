import pandas as pd

# List of words to pair with products
words = ['buy', 'price', 'discount', 'promotion', 'promo', 'shop']

# Print list of words
print(words)

products = ['sofas', 'convertible sofas', 'love seats', 'recliners', 'sofa beds']

# Create an empty list
keywords_list = []

# Loop through products
for product in products:
    # Loop through words
    for word in words:
        # Append combinations
        keywords_list.append([product, product + ' ' + word])
        keywords_list.append([product, word + ' ' + product])
        
# Inspect keyword list
print(keywords_list)

# Create a DataFrame from list
keywords_df = pd.DataFrame(keywords_list, columns=['Ad Group', 'Keyword'])

# Print the keywords DataFrame to explore it
print(keywords_df)

# Add a campaign column
keywords_df['Campaign'] = 'SEM_Sofas'

# Add a criterion type column
keywords_df['Criterion Type'] = 'Exact'

# Save the final keywords to a CSV file
keywords_df.to_csv('keywords.csv', index=False)
