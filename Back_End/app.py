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

try:
    mel = pd.read_pickle('Model/MEL.pkl')
    mhs = pd.read_pickle('Model/MHS.pkl')
    mbk = pd.read_pickle('Model/MBK.pkl')
    print('<<File is Connected>>')
except IOError:
    print('>>File not Found<<')



app = Flask(__name__, static_folder='static')

@app.route('/result', methods=['GET'])
def category():
    agrs = request.args
    user = agrs['user_id']
    query_1 = '''
    select distinct user_fingerprint, adlist_id, filter_category_id
    from hanhvi 
    where user_fingerprint = ''' + str(user) + ''' 
    order by event_server_time DESC
    limit 1;
    '''

    status = 'Connected!'
    res = pd.read_sql_query(query_1, conn).values.tolist()
    category = []
    for i in range(len(res)):
        val = [res[i][0], res[i][1]]
        if res[i][2] in range(1000,2000):
            category.append(mhs.predict([val])[0])
        elif res[i][2] in range(2000,3000):
            category.append(mbk.predict([val])[0])
        elif res[i][2] in range(5000,6000):
            category.append(mel.predict([val])[0])
        else: status = 'Not Connection!'
    list_ad = []
    col_name = ["ad_id", "list_id", "date", "account_name", "image", "subject", "category", "price_string", "area_name"]
    for j in category:
        query_2 = '''
        select distinct *
        from sanpham 
        where category = ''' + str(j) + ''' 
        order by date DESC
        limit 10;
        '''
        pro = pd.read_sql_query(query_2, conn).values.tolist()
        for k in range(len(pro)):
            jsc = {
                'category': str(j),
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
    app.run(host='localhost', port=80, debug=True)