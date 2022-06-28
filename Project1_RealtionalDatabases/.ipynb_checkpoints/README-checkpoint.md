# Overall Goal of Project
The goal of this project is to extract the necessary information from song_files and log_files to create a star-schema-like songplay database.

# Instructions for how to run the Python Code
To run the python code, 
* run "python create_tables.py" as a shell command or in the etl.ipynb (for some reason, I was not able to run it directly from test.ipynb)
* please run test.ipynb to create the tables with create_tables.py and insert the data by running etl.py. 
* Then, use the provided queries to check if inserting the data was successful.

# Meaning of different data sources
The song_files are used to fill the songs and artist tables while the lof_files are used to fill extract data for the user, time and songplays tables.