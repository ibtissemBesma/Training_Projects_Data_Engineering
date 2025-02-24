# Import the necessary libraries
import pandas as pd
import matplotlib.pyplot as plt

# Find the peak for global 'workout' searches
df_workout = pd.read_csv("data/workout.csv")

plt.figure(figsize=(12, 6))
plt.plot(df_workout["month"], df_workout["workout_worldwide"])
plt.xticks(rotation=90)
plt.show()

year_str = "2020"

# Find the most popular keywords for the current year and during covid
df_keywords = pd.read_csv("data/three_keywords.csv")

plt.figure(figsize=(12, 6))
plt.plot(df_keywords["month"], df_keywords["home_workout_worldwide"], label="Home workout")
plt.plot(df_keywords["month"], df_keywords["gym_workout_worldwide"], label="Gym workout")
plt.plot(df_keywords["month"], df_keywords["home_gym_worldwide"], label="Home gym")
plt.xticks(rotation=90)
plt.legend()
plt.show()

peak_covid = "home workout"
current = "gym workout"

# Find the country with the highest interest for workouts
df_workout_geo = pd.read_csv("data/workout_geo.csv", index_col = 0)
print(df_workout_geo.loc["United States"])
print(df_workout_geo.loc["Australia"])
print(df_workout_geo.loc["Japan"])

top_country = "United States"

# Who has the highest interest in home workouts, Philippines or Malaysia?
df_keywords_geo = pd.read_csv("data/three_keywords_geo.csv", index_col = 0)
print(df_keywords_geo.loc["Philippines", :])
print(df_keywords_geo.loc["Malaysia", :])

home_workout_geo = "Philippines"