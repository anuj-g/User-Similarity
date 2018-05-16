The aim of the project is to find information of similar users given a user. The project uses online learning platform dataset.

The user dataset consits of assessments taken by users, their scores in those assessment, courses taken by users, tags associated with those courses, user interests etc. The project uses all these features in the dataset, apply normalization methods and calcualte similarity between users.

The REST API is implemented to give information of similar users given the query user.

Dependencies:
1) Python 3.x
2) psycopg2
3) numpy
4) scipy
5) sklearn

Steps to run the program:
(Change username and password of postgre server in following files)
1) Put all csv files in data folder
2) Run command - python dataindexing.py
3) Run command - python similaritymatrix.py
4) Run command - python api.py
5) To get similar users: http://localhost:3041/userhandle/123 - 123 is query user handle