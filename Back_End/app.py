import os
import pickle
import pandas as pd
import sqlite3
from flask import Flask, render_template, request, jsonify, redirect, url_for

#connect data
conn = sqlite3.connect('Data/data.db', check_same_thread=False)
query = ''

#import model
mel = ''
mbk = ''
mhs = ''

#load model
try:
    mel = pd.read_pickle('Model/MEL.pkl')
    mhs = pd.read_pickle('Model/MHS.pkl')
    mbk = pd.read_pickle('Model/MBK.pkl')
    print('<<File is Connected>>')
except IOError:
    print('>>File not Found<<')


#API
app = Flask(__name__, static_folder='static')

#result
@app.route('/result', methods=['GET'])
def category():
    #Get values of user
    agrs = request.args
    user = agrs['user_id']

    temp = []
    list_ad = [] #post list ad
    status = 'Disconnected!'                                                            #post status
    col_name = ["account_name", "date", "image", "subject", "price_string", "area_name", "ad_id", "list_id", "category"] #name columns
    cate = [[],[],[]]                                                                   #list category of group category

    #add values to list category
    for i in range(5):
        n = '10' + str(i) + '0'
        cate[2].append(n)
    for i in range(9):
        x = '20' + str(i) + '0'
        d = '50' + str(i) + '0'
        cate[1].append(x)
        cate[0].append(d)
    
    # query = 'select count(*) from hanhvi where user_fingerprint == ' + str(user)        #query to get all users
    # res = pd.read_sql_query(query, conn)                                                #load user

    if len(user) > 3:                                                                   #check user id is positive
        # if res['count(*)'].values[0] == 0:                                              #check user new or old
        status = 'Connected by New User!'

        for i in range(len(cate)):                                                  #get new list ad
            for j in cate[i]:
                q = '''
                select *
                from sanpham 
                where category = ''' + str(j) + '''
                order by date DESC
                limit 1
                '''
                val = pd.read_sql_query(q, conn).values.tolist()
                temp.append(val)

        top = []
        for i in temp:
            if len(i) > 0: top.append(i[0])
        for k in range(len(top)):
                jsc = {
                    str(col_name[0]): str(top[k][0]),
                    str(col_name[1]): str(top[k][1]),
                    str(col_name[2]): str(top[k][2]),
                    str(col_name[3]): str(top[k][3]),
                    str(col_name[4]): str(top[k][4]),
                    str(col_name[5]): str(top[k][5]),
                    str(col_name[6]): str(top[k][6]),
                    str(col_name[7]): str(top[k][7]),
                    str(col_name[8]): str(top[k][8]),
                }
                list_ad.append(jsc)
    else: 
        status = 'Connected by Old User!'

        query_1 = '''
        select distinct adlist_id, filter_category_id
        from hanhvi 
        where user_fingerprint = ''' + str(user) + ''' 
        order by event_server_time DESC
        limit 1;
        '''

        res = pd.read_sql_query(query_1, conn).values.tolist()
        category = []
        for i in range(len(res)):
            val = [int(user), res[i][0]]
            if res[i][1] in range(1000,2000):
                category.append(mhs.predict([val])[0])
            elif res[i][1] in range(2000,3000):
                category.append(mbk.predict([val])[0])
            elif res[i][1] in range(5000,6000):
                category.append(mel.predict([val])[0])
            else: status = 'Values is not called from model!'
        
        for j in category:
            query_2 = '''
            select distinct *
            from sanpham 
            where category = ''' + str(j) + '''
            limit 10;
            '''
            pro = pd.read_sql_query(query_2, conn).values.tolist()
            for k in range(len(pro)):
                jsc = {
                    str(col_name[0]): str(pro[k][0]),
                    str(col_name[1]): str(pro[k][1]),
                    str(col_name[2]): str(pro[k][2]),
                    str(col_name[3]): str(pro[k][3]),
                    str(col_name[4]): str(pro[k][4]),
                    str(col_name[5]): str(pro[k][5]),
                    str(col_name[6]): str(pro[k][6]),
                    str(col_name[7]): str(pro[k][7]),
                    str(col_name[8]): str(pro[k][8]),
                }
                list_ad.append(jsc)
    

    return jsonify({
        'state': status,
        'list': list_ad,
        'user_id': user
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4500, debug=True)
