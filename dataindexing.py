import psycopg2
import csv
from datetime import datetime

def getopenconnection(user='postgres', password='postgres', dbname='iris'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' \
        host='localhost' password='" + password + "'")

#indexing assessment score data
def insertAssessmentScore(openconnection, tablename='user_assessment_scores'):
    with open('data/user_assessment_scores.csv', newline='\n') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)
        cur = openconnection.cursor()
        for row in reader:
            row_date = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")
            cur.execute("INSERT INTO {0} (user_handle,assessment_tag,\
                        assessment_date,assessment_score) VALUES({1}, \
                        '{2}','{3}',{4});".format(tablename, int(row[0]), \
                        row[1], row_date, row[3]))
        cur.close()

#indexing course score data
def insertCourseTags(openconnection, tablename='course_tags'):
    with open('data/course_tags.csv', newline='\n') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)
        cur = openconnection.cursor()
        for row in reader:
            cur.execute("INSERT INTO {0} (course_name,tag) VALUES('{1}','{2}' \
                        );".format(tablename, row[0], row[1]))
        cur.close()

#indexing course views data
def insertUserCourseViews(openconnection, tablename='user_course_views'):
    with open('data/user_course_views.csv', newline='\n') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)
        cur = openconnection.cursor()
        for row in reader:
            row_date = datetime.strptime(row[1], "%Y-%m-%d")
            cur.execute("INSERT INTO {0} (user_handle, view_date, course_name, \
                        author_handle, level, course_view_time_seconds) VALUES \
                        ({1},'{2}','{3}',{4},'{5}',{6});".format(tablename, \
                        int(row[0]), row_date, row[2], row[3], row[4], row[5]))
        cur.close()

#indexing user interest data
def insertUserInterests(openconnection, tablename='user_interests'):
    with open('data/user_interests.csv', newline='\n') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)
        cur = openconnection.cursor()
        for row in reader:
            row_date = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")
            cur.execute("INSERT INTO {0} (user_handle,interest_tag,\
                        date_followed) VALUES({1}, '{2}','{3}');" \
                        .format(tablename, int(row[0]), \
                        row[1], row_date))
        cur.close()

if __name__ == "__main__":
    try:
        with getopenconnection() as con:
            insertassessmentscore(con)
            insertcoursetags(con)
            insertusercourseviews(con)
            insertuserinterests(con)
    except Exception as e:
        print(e)