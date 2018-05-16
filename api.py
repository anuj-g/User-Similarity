from flask import Flask
import psycopg2
import json
from flask import jsonify
app = Flask(__name__)

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
       if isinstance(obj, set):
          return list(obj)
       return json.JSONEncoder.default(self, obj)

@app.route('/userhandle/<int:user_handle>')
def getSimilarUsers(user_handle):
    try:
        result = {}
        conn = psycopg2.connect("dbname='iris' user='postgres' host='localhost'\
            password='postgres'")
        cur = conn.cursor()
        query = """SELECT * from similar_users where similar_users.user =""" \
            + str(user_handle)
        cur.execute(query)
        globalrows = cur.fetchone()
       
        for i in range(3):
            queryone="""SELECT * from  user_assessment_scores where \
            user_handle =""" + str(globalrows[i+1])
            cur.execute(queryone)
            rows = cur.fetchall()
           
            data={}
            assessments=[]
            for row in rows:
                single_value={}
                date={}
                score={}
                single_value_data=[]
                date['assessment_date']=row[2]  
                score['assessment_score']=row[3]    
                single_value_data.append(str(date))
                single_value_data.append(score)
            
                single_value[row[1]]=single_value_data
                assessments.append(single_value)
            data['assessment'] = assessments
            
            querytwo="""SELECT * from  user_course_views ucv INNER JOIN \
            course_tags ct ON ucv.course_name=ct.course_name where \
            ucv.user_handle = """ + str(globalrows[i+1])
            
            cur.execute(querytwo)
            rows = cur.fetchall()
            course_views={}
            for row in rows:
                if row[2] not in course_views:
                    single_value_data={}
                    level={}
                    timeSpent={}
                    single_value_data['level']=row[4]
                    tags=set()
                    tags.add(row[7])
                    date_time_seconds={}
                    date_time_seconds['date']=str(row[1])
                    date_time_seconds['seconds_spent']=row[5]
                    single_value_data['tag']=tags
                    single_value_data['course_view_time_seconds']=[date_time_seconds]
                    course_views[row[2]]=single_value_data
                else:
                    date_time_seconds={}
                    date_time_seconds['date']=str(row[1])
                    date_time_seconds['seconds_spent']=row[5]
                    course_views[row[2]]['course_view_time_seconds'].append(date_time_seconds)
                    course_views[row[2]]['tag'].add(row[7]) 
            
            json_data = json.dumps(course_views,cls=SetEncoder)
            data['course_views']=json_data


            queryThree="""SELECT * from  user_interests where user_handle = \
            """ + str(globalrows[i+1])
            cur.execute(queryThree)
            rows = cur.fetchall()
            interests=[]
            for row in rows:
                interest={}
                interest['interest_tag']=row[1]
                interest['date_followed']=row[2]
                interests.append(interest)
            data['user_interests']=interests
            result[globalrows[i+1]] = data
        
        cur.close()
    except Exception as e:
        print(e)
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3041)