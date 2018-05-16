import psycopg2
import numpy as np
import pandas as pd
from scipy.spatial.distance import squareform
from scipy.spatial.distance import pdist, jaccard
from sklearn.preprocessing import normalize
import csv

#initialize variables to build feature matrix
users_data = {}
unique_interest_tags = set()
unique_assessment_tags = set()
unique_courses = set()
difficulty_levels = set()
unique_course_tags = set()

#Returns connection to Postgres
def getopenconnection(user='postgres', password='postgres', dbname='iris'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' \
        host='localhost' password='" + password + "'")

#Stores user interest data in users_data dict
def storeuserinterestdata(openconnection):
    cur = openconnection.cursor()
    cur.execute("select * from user_interests")
    for record in cur.fetchall():
        user_handle = record[0]
        interest_tag = record[1]
        if user_handle not in users_data:
            users_data[user_handle] = {}
            users_data[user_handle]['interest'] = {}
        if interest_tag not in users_data[user_handle]['interest']:
            users_data[user_handle]['interest'][interest_tag] = 1
        unique_interest_tags.add(interest_tag)

#Stores user assessment data in user_data dict
def storeassessmentdata(openconnection):
    cur = openconnection.cursor()
    cur.execute("select * from user_assessment_scores")
    for record in cur.fetchall():
        user_handle = record[0]
        assessment_tag = record[1]
        if user_handle not in users_data:
            users_data[user_handle] = {}
        if 'assessment' not in users_data[user_handle]:
            users_data[user_handle]['assessment'] = {}
        if assessment_tag not in users_data[user_handle]['assessment']:
            users_data[user_handle]['assessment'][assessment_tag] = record[3]
        unique_assessment_tags.add(assessment_tag)

'''
Stores user course data in user_data dict
Stores the total time spent by user on a course
Stores the difficulty count of courses takes by the user
Stores the course tag counts associated with courses taken by the user
'''
def storeusercoursedata(openconnection):
    cur = openconnection.cursor()
    cur.execute("select * from user_course_views as u inner join course_tags \
        as c on u.course_name = c.course_name")
    user_course_data = set()
    for record in cur.fetchall():
        user_handle = record[0]
        course_name = record[2]
        course_level = record[4]
        course_time = record[5]
        course_tag = record[7]
        course_time_key = course_name + str(course_time)
        course_tag_key = course_name + course_tag
        if user_handle not in users_data:
            users_data[user_handle] = {}
            user_course_data = set()
        if 'courses' not in users_data[user_handle]:
            users_data[user_handle]['courses'] = {}
        if course_name not in users_data[user_handle]['courses']:
            users_data[user_handle]['courses'][course_name] = 0
            if course_level not in users_data[user_handle]['courses']:
                users_data[user_handle]['courses'][course_level] = 0
            users_data[user_handle]['courses'][course_level] += 1

        if course_time_key not in user_course_data:
            users_data[user_handle]['courses'][course_name] += course_time
        
        if course_tag_key not in user_course_data:
            if course_tag not in users_data[user_handle]['courses']:
                users_data[user_handle]['courses'][course_tag] = 0
            users_data[user_handle]['courses'][course_tag] += 1

        user_course_data.add(course_tag_key)
        user_course_data.add(course_time_key)

        unique_courses.add(course_name)
        difficulty_levels.add(course_level)
        unique_course_tags.add(course_tag)

'''
feature matrix contains following features
- assessment_tag with value as score in that assessment
- course_name with value as time spent on that course by that user
- course_tag with value as count of that tag associated with the user
- difficulty_level with value as number of courses with that difficulty taken by a user
- interest_tag with binary value if the user is interested in that tag
For the given data feature matrix dimension is 10000 * 7648
'''
def buildfeaturematrix():
    user_ids = []
    global unique_assessment_tags
    global unique_interest_tags
    global unique_course_tags
    global unique_courses
    global difficulty_levels

    unique_assessment_tags = list(unique_assessment_tags)
    unique_interest_tags = list(unique_interest_tags)
    unique_course_tags = list(unique_course_tags)
    unique_courses = list(unique_courses)
    difficulty_levels = list(difficulty_levels)
    feature_matrix = np.zeros(shape=(len(users_data.keys()), \
        len(unique_assessment_tags) + len(unique_interest_tags) + \
        len(unique_course_tags) + len(unique_courses) + \
        len(difficulty_levels)), dtype=float)
    
    fm_row_index = 0
    for user in users_data:
        fm_col_index = 0
        user_ids.append(user)
        if 'assessment' in users_data[user]:
            for assessment_tag in unique_assessment_tags:
                if assessment_tag in users_data[user]['assessment']:
                    feature_matrix[fm_row_index][fm_col_index] = \
                    users_data[user]['assessment'][assessment_tag]
                fm_col_index += 1
        else:
            fm_col_index += len(unique_assessment_tags)
        
        if 'interest' in users_data[user]:
            for interest_tag in unique_interest_tags:
                if interest_tag in users_data[user]['interest']:
                    feature_matrix[fm_row_index][fm_col_index] = \
                    users_data[user]['interest'][interest_tag]
                fm_col_index += 1
        else:
            fm_col_index += len(unique_interest_tags)
        
        if 'courses' in users_data[user]:
            for course_tag in unique_course_tags:
                if course_tag in users_data[user]['courses']:
                    feature_matrix[fm_row_index][fm_col_index] = \
                    users_data[user]['courses'][course_tag]
                fm_col_index += 1
            for course in unique_courses:
                if course in users_data[user]['courses']:
                    feature_matrix[fm_row_index][fm_col_index] = \
                    users_data[user]['courses'][course]
            for level in difficulty_levels:
                if level in users_data[user]['courses']:
                    feature_matrix[fm_row_index][fm_col_index] = \
                    users_data[user]['courses'][level]
        fm_row_index += 1
    return user_ids, feature_matrix

'''
Normalizes all the features in feature matrix
Calculates similarity between users using cosine similarity
Stores similar user matrix in csv format
'''
def calculatesimilarity(user_ids, feature_matrix):
    res = 1 - pdist(feature_matrix, 'cosine')
    similarity_matrix = squareform(res)
    user_ids = np.asarray(user_ids, dtype=np.int32)
    user_ids = user_ids.reshape(user_ids.shape[0], 1)
    similarusers = np.concatenate((user_ids, similarity_matrix.argsort() + 1), axis=1)
    return similarusers

'''
Stores similar users in table similar_users
'''
def indexsimilarusers(openconnection, similarusers):
    users = similarusers[:,0]
    userone = similarusers[:,-1]
    usertwo = similarusers[:,-2]
    userthree = similarusers[:,-3]
    sql='''
        INSERT INTO similar_users SELECT unnest( %(users)s ) , \
            unnest( %(userone)s) , unnest( %(usertwo)s) , \
            unnest( %(userthree)s)
    '''
    cur = openconnection.cursor()
    cur.execute(sql, {
        'users': users.tolist(),
        'userone': userone.tolist(),
        'usertwo': usertwo.tolist(),
        'userthree': userthree.tolist()
    })

if __name__ == "__main__":
    with getopenconnection() as con:
        storeuserinterestdata(con)
        storeassessmentdata(con)
        storeusercoursedata(con)
        user_ids, feature_matrix = buildfeaturematrix()
        similar_users = calculatesimilarity(user_ids, feature_matrix)
        indexsimilarusers(con, similar_users)