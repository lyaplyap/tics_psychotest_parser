import requests
import csv
import pymysql

# Получение результатов из БД
def from_database():
    connection = pymysql.connect(

        host = "",
        user = "",
        password = "",
        db = "",
        charset = ""
    )

    cur = connection.cursor()
    cur.execute("SELECT * FROM bigfivetest")
    bigfive_rows = cur.fetchall()

    cur.execute("SELECT * FROM schwartztest") 
    schw_rows = cur.fetchall()

    cur.execute("SELECT * FROM kettelltest") 
    ket_rows = cur.fetchall()

    cur.execute("SELECT * FROM defensetest") 
    def_rows = cur.fetchall()

    connection.close()

    # Представление результатов Большой Пятерки в виде массива словарей
    data_bigfive = []
    for row in bigfive_rows:
        is_exist = False
        for data in data_bigfive:
            if row[6] == data['id']:
                is_exist = True
        if is_exist == False:
            data_bigfive.append({'id': row[6], 'results_bigfive': [row[1], row[2], row[3], row[4], row[5]]})

    # Представление результатов опросника Шварца в виде массива словарей
    data_schw = []
    for row in schw_rows:
        is_exist = False
        for data in data_schw:
            if row[21] == data['id']:
                is_exist = True
        if is_exist == False:
            data_schw.append({'id': row[21], 'results_schwartz': [row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], \
                                                         row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20]]})

    # Представление результатов опросника Кеттелла в виде массива словарей
    data_ket = []
    for row in ket_rows:
        is_exist = False
        for data in data_ket:
            if row[17] == data['id']:
                is_exist = True
        if is_exist == False:
            data_ket.append({'id': row[17], 'results_kettell': [row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], \
                                                         row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16]]})

    # Представление результатов Псих. Защиты в виде массива словарей
    data_def = []
    for row in def_rows:
        is_exist = False
        for data in data_def:
            if row[10] == data['id']:
                is_exist = True
        if is_exist == False:
            data_def.append({'id': row[10], 'results_defense': [row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]]})
    

    # Добавление результатов Кеттелла
    db_data = data_def.copy()
    for ket in data_ket:
        is_exist = False
        for db in db_data:
            if db['id'] == ket['id']:
                db['results_kettell'] = ket['results_kettell']
                is_exist = True
        if is_exist == False:
            db_data.append({'id': ket['id'], 'results_defense': ['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un'], 'results_kettell': ket['results_kettell']})
    
    for data in db_data:
        if len(data) < 3:
            data['results_kettell'] = ['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un']

    # Добавление результатов Шварца
    for schw in data_schw:
        is_exist = False
        for db in db_data:
            if db['id'] == schw['id']:
                db['results_schwartz'] = schw['results_schwartz']
                is_exist = True
        if is_exist == False:
            db_data.append({'id': schw['id'], 'results_defense': ['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un'], \
                            'results_kettell': ['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un'], 'results_schwartz': schw['results_schwartz']})
    
    for data in db_data:
        if len(data) < 4:
            data['results_schwartz'] = ['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un']

    # Добавление результатов Большой Пятёрки
    for bigfive in data_bigfive:
        is_exist = False
        for db in db_data:
            if db['id'] == bigfive['id']:
                db['results_bigfive'] = bigfive['results_bigfive']
                is_exist = True
        if is_exist == False:
            db_data.append({'id': bigfive['id'], 'results_defense': ['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un'], \
                            'results_kettell': ['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un'], \
                            'results_schwartz': ['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un'], \
                            'results_bigfive': bigfive['results_bigfive']})
    
    for data in db_data:
        if len(data) < 5:
            data['results_bigfive'] = ['un', 'un', 'un', 'un', 'un']

    
    # Тестовый вывод
    for data in db_data:
        print(data['id'], data['results_defense'], data['results_kettell'], data['results_schwartz'], data['results_bigfive'])
        print()

    return db_data

# Получение данных из профилей
def take_data(db_data):

    token = ''
    user_token = ''
    version = 5.89
    all_data = []

    list_id = []
    for item in db_data:
        list_id.append(item['id'])

    for id in list_id:
        response = requests.get('https://api.vk.com/method/users.get',
                                params={
                                    'access_token': token,
                                    'v': version,
                                    'user_id': id,
                                    'fields': 'sex,bdate,career,city,educations,occupation,personal,relation,schools,universities'
                                    }
                                )

        data = response.json()['response'][0]

        response = requests.get('https://api.vk.com/method/users.get', 
                                params={
                                    'access_token': user_token,
                                    'v': version,
                                    'user_id': id,
                                    'fields': 'counters'
                                    }
                                )
        counters = response.json()['response'][0]['counters']

        all_data.append({ 'vk': data, 'counters': counters })

    return all_data

# Запись данных в CSV-файл
def file_writer(db_data, data):
    i = 0
    with open('psycho_datasets.csv', 'w', encoding='utf-8') as file:
        a_pen = csv.writer(file)
        a_pen.writerow(('id', 'first_name', 'last_name', 'is_closed', 'sex', 'bdate', 'friends', 'followers', 'photos', 'audios', 'pages', 'occupation', \
                        'def1', 'def2', 'def3', 'def4', 'def5', 'def6', 'def7', 'def8', 'def9', \
                        'ket1', 'ket2', 'ket3', 'ket4', 'ket5', 'ket6', 'ket7', 'ket8', 'ket9', 'ket10', 'ket11', 'ket12', 'ket13', 'ket14', 'ket15', 'ket16', \
                        'rang1-1', 'rang1-2', 'rang1-3', 'rang1-4', 'rang1-5', 'rang1-6', 'rang1-7', 'rang1-8', 'rang1-9', 'rang1-10', 'rang2-1', 'rang2-2', 'rang2-3', 'rang2-4', 'rang2-5', 'rang2-6', 'rang2-7', 'rang2-8', 'rang2-9', 'rang2-10', \
                        'bigfive1_extraversion', 'bigfive2_affection', 'bigfive3_selfcontrol', 'bigfive4_emotstability', 'bigfive5_expressiveness'))
        for user in data:
            a_pen.writerow((user['vk']['id'], user['vk']['first_name'], user['vk']['last_name'], user['vk']['is_closed'], user['vk']['sex'], user['vk']['bdate'] if 'bdate' in user['vk'] else "undefined", \
                            user['counters']['friends'], user['counters']['followers'] if 'followers' in user['counters'] else 'undefined', user['counters']['photos'] if 'photos' in user['counters'] else 'undefined', \
                            user['counters']['audios'] if 'audios' in user['counters'] else 'undefined', user['counters']['pages'] if 'pages' in user['counters'] else 'undefined', user['vk']['occupation']['name'] if 'occupation' in user['vk'] else 'undefined', \
                            db_data[i]['results_defense'][0], \
                            db_data[i]['results_defense'][1], \
                            db_data[i]['results_defense'][2], \
                            db_data[i]['results_defense'][3], \
                            db_data[i]['results_defense'][4], \
                            db_data[i]['results_defense'][5], \
                            db_data[i]['results_defense'][6], \
                            db_data[i]['results_defense'][7], \
                            db_data[i]['results_defense'][8], \
                            db_data[i]['results_kettell'][0], \
                            db_data[i]['results_kettell'][1], \
                            db_data[i]['results_kettell'][2], \
                            db_data[i]['results_kettell'][3], \
                            db_data[i]['results_kettell'][4], \
                            db_data[i]['results_kettell'][5], \
                            db_data[i]['results_kettell'][6], \
                            db_data[i]['results_kettell'][7], \
                            db_data[i]['results_kettell'][8], \
                            db_data[i]['results_kettell'][9], \
                            db_data[i]['results_kettell'][10], \
                            db_data[i]['results_kettell'][11], \
                            db_data[i]['results_kettell'][12], \
                            db_data[i]['results_kettell'][13], \
                            db_data[i]['results_kettell'][14], \
                            db_data[i]['results_kettell'][15], \
                            db_data[i]['results_schwartz'][0], \
                            db_data[i]['results_schwartz'][1], \
                            db_data[i]['results_schwartz'][2], \
                            db_data[i]['results_schwartz'][3], \
                            db_data[i]['results_schwartz'][4], \
                            db_data[i]['results_schwartz'][5], \
                            db_data[i]['results_schwartz'][6], \
                            db_data[i]['results_schwartz'][7], \
                            db_data[i]['results_schwartz'][8], \
                            db_data[i]['results_schwartz'][9], \
                            db_data[i]['results_schwartz'][10], \
                            db_data[i]['results_schwartz'][11], \
                            db_data[i]['results_schwartz'][12], \
                            db_data[i]['results_schwartz'][13], \
                            db_data[i]['results_schwartz'][14], \
                            db_data[i]['results_schwartz'][15], \
                            db_data[i]['results_schwartz'][16], \
                            db_data[i]['results_schwartz'][17], \
                            db_data[i]['results_schwartz'][18], \
                            db_data[i]['results_schwartz'][19], \
                            db_data[i]['results_bigfive'][0], \
                            db_data[i]['results_bigfive'][1], \
                            db_data[i]['results_bigfive'][2], \
                            db_data[i]['results_bigfive'][3], \
                            db_data[i]['results_bigfive'][4]))
            i += 1
            

db_data = from_database()
all_data = take_data(db_data)
file_writer(db_data, all_data)
