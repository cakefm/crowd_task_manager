# get slices from db
# list slices on home page
# make api endpoint for slice
# each shows slice image and xml, create this from template
from flask import Flask
from flask import render_template
app = Flask(__name__)
import pymongo
import re
import os
import yaml
import urllib.request
import pathlib
from flask import Flask
from flask import render_template
from bson.objectid import ObjectId
from flask import request
from flask import jsonify
from datetime import datetime
from flask import flash, redirect, url_for
from werkzeug.utils import secure_filename
from flask import send_from_directory
from pathlib import Path
from lxml import etree
from xmldiff import main
from urllib.parse import urlparse
from shutil import copyfile
import logging
import ssl
import pika
import sys
import json
sys.path.append("..")
import common.settings as settings
import common.file_system_manager as fsm
import pwd
import grp

with open("../settings.yaml", 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

rabbitmq_address = cfg['rabbitmq_address']
address = rabbitmq_address.split(":")
path = os.getcwd()
UPLOAD_FOLDER_TEMP = path + '/uploads'
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}
MONGO_SERVER = cfg['mongo_server']
MONGO_DB = cfg['mongo_db']
TASK_COLL = cfg['mongo_task_collection']
# CLIENT_SECRET = cfg['client_secret']
CURRENT_SERVER = cfg['current_server']

app.config['UPLOAD_FOLDER'] = str(Path.home()) + cfg['upload_folder']


@app.route('/')
# display all the tasks
@app.route('/index')
def index():
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb['submitted_tasks']
    myquery = {}
    mydoc = mycol.find(myquery)
    tasks = []
    for x in mydoc:
        tasks.append(x)
    return render_template('index.html', title='Home', tasks=tasks)


@app.route('/tasks', methods=['GET'])
def get_tasks():
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb['tasks']
    myquery = {}
    mydoc = mycol.find(myquery)
    tasks = []
    for x in mydoc:
        task = {}
        task['_id'] = str(x['_id'])
        task['type'] = 'edit' if 'type' not in x else x['type']
        tasks.append(task)
    resp = jsonify(tasks=tasks)
    return resp


@app.route('/tasks/<variable>', methods=['GET'])
def get_task_query(variable):
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb['tasks']
    myquery = {"_id": ObjectId(variable)}
    mydoc = mycol.find_one(myquery)
    task = {}
    task['task_id'] = variable
    task['image_url'] = CURRENT_SERVER + 'static/' + mydoc['image_path']
    task['mei_snippet'] = mydoc['xml']

    mycol = mydb['task_context']
    myquery = {"task_id": variable}
    mydoc = mycol.find_one(myquery)
    task['preface'] = mydoc['preface']
    task['postface'] = mydoc['postface']

    # resp = jsonify(task=task)
    return task


# display task info, slice, and xml
@app.route('/edit/<variable>', methods=['GET'])
def task(variable):
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb[TASK_COLL]
    myquery = {"_id": ObjectId(variable)}
    mydoc = mycol.find(myquery)
    task = mydoc[0]
    xml = task['xml']
    xml2 = re.sub(r'\s+', ' ', xml)
    print(xml2)
    return render_template("task.html", task=task, xml=xml2)


# display task info, slice, and xml
@app.route('/verify/<variable>', methods=['GET'])
def task_verify(variable):
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb[TASK_COLL]
    myquery = {"_id": ObjectId(variable)}
    mydoc = mycol.find(myquery)
    task = mydoc[0]
    xml = task['xml']
    xml2 = re.sub(r'\s+', ' ', xml)
    print(xml2)
    return render_template("task_verify.html", task=task, xml=xml2)


# getxml data
@app.route('/xml/<variable>', methods=['GET'])
def task_xml(variable):
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb[TASK_COLL]
    myquery = {"_id": ObjectId(variable)}
    mydoc = mycol.find(myquery)
    task = mydoc[0]
    xml = task['xml']
    # print(xml)
    # print(type(xml))
    resp = jsonify(xml=xml)
    return resp


# receive xml data
@app.route('/<variable>', methods=['POST'])
def taskpost(variable):
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb["results"]
    opinion = 'xml' if 'v' not in request.args else (request.args['v'] == "1")
    result = {
        "task_id": variable,
        "xml": str(request.get_data(as_text=True)),
        "ts": datetime.now(),
        "worker": "somebody" if 'u' not in request.args else request.args['u'],
        "result_type": "verify" if 'v' in request.args else "edit",
        "opinion": 'xml' if 'v' not in request.args else (request.args['v'] == "1")
    }
    entry = mycol.insert_one(result)

    # check if the task is complete
    mycol_other = mydb['submitted_tasks']
    task_status = mycol_other.find({"task_id": variable, "type": result['result_type']})
    if task_status.count() > 0:
        if task_status[0]['status'] != "complete" and opinion == 'xml':
            xml_in = str(request.get_data(as_text=True))
            other_entry = mycol_other.update_one(
                {"task_id": variable, "type": result['result_type']},
                {'$push': {'xml': xml_in}},
                upsert=True)
            count = len(task_status[0]['xml'])
            if(count == 1):
                # set status of controlaction to active
                send_message(
                    'ce_communicator_queue',
                    'ce_communicator_queue',
                    json.dumps({
                        'action': 'task active',
                        'identifier': variable,
                        'type': 'edit',
                        'status': 'ActiveActionStatus'}))
            if(count > 1):
                x = 0
                for x in range(0, count - 1):
                    xml_string1 = re.sub(r'\s+', ' ', task_status[0]['xml'][x])
                    xml_string2 = re.sub(r'\s+', ' ', xml_in)

                    tree1 = etree.fromstring(xml_string1)
                    tree2 = etree.fromstring(xml_string2)
                    diff = main.diff_trees(tree1, tree2)
                    # check is the new one matches one of the known ones
                    print("compare xml diff ", len(diff))
                    if(len(diff) == 0):
                        # if two match then save the result
                        # in result_agg collection
                        results_agg_coll = mydb['results_agg']
                        good_result = {
                            "task_id": variable,
                            "xml": xml_string2
                        }
                        results_agg_coll.insert_one(good_result)
                        # mark the submitted task done
                        mycol_other.update_one(
                            {
                                "task_id": variable
                            }, {
                                '$set': {
                                    'status': "complete"
                                }
                            })
                        # send message to omr_planner
                        tasks_coll = mydb['tasks']
                        task = tasks_coll.find_one({"_id": ObjectId(variable)})
                        status_update_msg = {
                            '_id': variable,
                            'module': 'aggregator',
                            'status': 'complete',
                            'name': task['score']}
                        send_message(
                            'status_queue',
                            'status_queue',
                            json.dumps(status_update_msg))
                        send_message(
                            'ce_communicator_queue',
                            'ce_communicator_queue',
                            json.dumps({
                                'action': 'task completed',
                                'identifier': variable,
                                'type': 'edit',
                                'status': 'CompletedActionStatus'}))
                        if(opinion == 'xml'):
                            a = mydb['tasks']
                            xml_in = str(request.get_data(as_text=True))
                            c = a.update_one(
                                {"_id": ObjectId(variable)},
                                {'$set': {'xml': xml_in}},
                                upsert=True)
                            send_message(
                                'ce_communicator_queue',
                                'ce_communicator_queue',
                                json.dumps({
                                    'action': 'verify task created',
                                    '_id': variable}))
            if(count == 1):
                mycol_other.update_one(
                    {"task_id": variable, "type": result['result_type']},
                    {'$set': {'status': "processing"}})
        elif task_status[0]['status'] != "complete" and opinion != 'xml':
            mycoll = mydb['results']
            query = {"task_id": variable, 'result_type': 'verify'}
            # query = {"task_id": variable, "opinion": True}
            mydoc = mycol.find(query)
            if(mydoc.count() == 1):
                mycol_other.update_one(
                    {"task_id": variable, "type": result['result_type']},
                    {'$set': {'status': "processing"}})
                # set status of controlaction to active
                send_message(
                    'ce_communicator_queue',
                    'ce_communicator_queue',
                    json.dumps({
                        'action': 'task active',
                        'identifier': variable,
                        'type': 'verify',
                        'status': 'ActiveActionStatus'}))
            mycoll = mydb['results']
            query = {"task_id": variable, "opinion": True}
            if(mydoc.count() > 1):
                mycol_other.update_one(
                    {"task_id": variable, "type": result['result_type']},
                    {'$set': {'status': "complete"}})
                send_message(
                    'ce_communicator_queue',
                    'ce_communicator_queue',
                    json.dumps({
                        'action': 'task completed',
                        'identifier': variable,
                        'type': 'verify',
                        'status': 'CompletedActionStatus'}))
    resp = jsonify(success=True)
    return resp


# display list of completed sheets
@app.route('/results', methods=['GET'])
def index_sheets():
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb["results_agg"]
    myquery = {}
    mydoc = mycol.find(myquery)
    sheets = []
    for x in mydoc:
        sheets.append(x)
    return render_template('sheets.html', title='Home', sheets=sheets)


# display aggregated task results
@app.route('/results/<variable>', methods=['GET'])
def show_sheet(variable):
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb["results_agg"]
    myquery = {"_id": ObjectId(variable)}
    mydoc = mycol.find(myquery)
    sheet = mydoc[0]
    xml = sheet['xml']
    xml2 = re.sub(r'\s+', ' ', xml)
    return render_template("sheet.html", sheet=sheet, xml=xml2)


# display aggregated task results
@app.route('/context/<variable>', methods=['GET'])
def show_page_context(variable):
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb["task_context_test"]
    myquery = {"task_id": ObjectId(variable)}
    mydoc = mycol.find_one(myquery)
    page_nr = mydoc['page_nr']
    score = mydoc['score']
    coords = mydoc['coords']

    mycol2 = mydb['sheets']
    myquery2 = {"name": score}
    mydoc2 = mycol2.find_one(myquery2)
    nr_pages = len(mydoc2['pages_path'])

    mycol = mydb["task_context_test"]
    myquery = {"score": score, "page_nr": page_nr}
    mydoc = mycol.find(myquery, {'_id': False, 'task_id': False})

    tasks = []
    for x in mydoc:
        tasks.append(x)
    return render_template("page_context.html", tasks=json.dumps(tasks), coords=json.dumps(coords), nr_pages=nr_pages)


def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def send_message(queue_name, routing_key, message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=address[0],
            port=address[1]))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=routing_key, body=message)
    connection.close()


# endpoint for uploading pdf that kicks off the demo use case
@app.route('/upload', methods=['POST', 'GET'])
def upload_sheet():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        mei = request.files['mei']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            data_folder = Path(app.config['UPLOAD_FOLDER'])
            data_folder = data_folder / os.path.splitext(file.filename)[0]
            pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
            uid = pwd.getpwnam("ubuntu").pw_uid
            gid = grp.getgrnam("ubuntu").gr_gid
            os.chown(data_folder, uid, gid)
            data_folder = data_folder / "whole"
            sheet_path = data_folder / filename

            pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
            os.chown(data_folder, uid, gid)
            file.save(os.path.join(data_folder, sheet_path))
            os.chown(sheet_path, uid, gid)
            mei_path = ''

            if mei:
                mei_filename = secure_filename(mei.filename)
                mei_path = data_folder / mei_filename

                mei.save(os.path.join(data_folder, mei_filename))
                os.chown(mei_path, uid, gid)

            # create entry into database
            myclient = pymongo.MongoClient(MONGO_SERVER)
            mydb = myclient[MONGO_DB]
            mycol = mydb["sheets"]
            # copy file to omr_files
            data_folder_temp = Path(UPLOAD_FOLDER_TEMP)
            os.chown(data_folder_temp, uid, gid)
            pathlib.Path(data_folder_temp).mkdir(parents=True, exist_ok=True)
            sheet_path_temp = data_folder_temp / filename
            file.save(os.path.join(data_folder_temp, sheet_path_temp))
            os.chown(sheet_path_temp, uid, gid)
            sheet_path_temp = filename

            result = {
                "name": os.path.splitext(file.filename)[0],
                "description": request.form['description'],
                "sheet_path": str(sheet_path),
                "ts": datetime.now(),
                "submitted_mei_path": str(mei_path)
            }
            identifier = mycol.insert_one(result).inserted_id
            # send message to omr_planner
            message = {'score_name': os.path.splitext(filename)[0], '_id': str(identifier)}
            send_message(
                'omr_planner_queue',
                'omr_planner_queue',
                json.dumps(message))

            return redirect(url_for('uploaded_file', filename=sheet_path_temp))

    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload music score PDF</h1>
    <form method=post enctype=multipart/form-data>
        Name: <br>
        <input type=text name=name value="">
        <br>
        Description: <br>
        <input type=text name=description value=""><br>
        PDF: <br><input type=file name=file><br>
        MEI(Optional): <br><input type=file name=mei>
        <input type=submit value=Upload>
    </form>
    '''


@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(
        UPLOAD_FOLDER_TEMP,
        filename)


@app.route('/upload/url_submit', methods=['POST', 'GET'])
def download_from_url():
    print('Beginning file download with urllib2...')
    if request.method == 'POST':
        url = request.form['url']
        a = urlparse(url)
        filename = os.path.basename(a.path)
        extension = os.path.splitext(filename)[-1][1:]
        # check if the post request has the file part
        if 'url' not in request.form:
            flash('No file part')
            return redirect(request.url)
        if allowed_file(filename):
            filename = secure_filename(filename)
            path_whole_files = os.path.join(
                app.config['UPLOAD_FOLDER'],
                os.path.splitext(filename)[0])
            pathlib.Path(path_whole_files).mkdir(parents=True, exist_ok=True)
            uid = pwd.getpwnam("ubuntu").pw_uid
            gid = grp.getgrnam("ubuntu").gr_gid
            os.chown(path_whole_files, uid, gid)
            path_whole_files = os.path.join(
                path_whole_files,
                "whole")
            pathlib.Path(path_whole_files).mkdir(parents=True, exist_ok=True)
            os.chown(path_whole_files, uid, gid)
            sheet_path = os.path.join(path_whole_files, filename)
            urllib.request.urlretrieve(url, sheet_path)
            os.chown(sheet_path, uid, gid)

            # create entry into database
            myclient = pymongo.MongoClient(MONGO_SERVER)
            mydb = myclient[MONGO_DB]
            mycol = mydb["sheets"]
            # copy file to omr_files
            data_folder_temp = Path(UPLOAD_FOLDER_TEMP)
            pathlib.Path(data_folder_temp).mkdir(parents=True, exist_ok=True)
            os.chown(data_folder_temp, uid, gid)
            sheet_path_temp = data_folder_temp / filename
            copyfile(sheet_path, sheet_path_temp)
            os.chown(sheet_path_temp, uid, gid)
            sheet_path_temp = filename

            result = {
                "name": os.path.splitext(filename)[0],
                "description": request.form['description'],
                "sheet_path": str(sheet_path),
                "ts": datetime.now()
            }
            identifier = mycol.insert_one(result).inserted_id

            # send message to omr_planner
            message = {'score_name': os.path.splitext(filename)[0], '_id': str(identifier)}
            send_message(
                'omr_planner_queue',
                'omr_planner_queue',
                json.dumps(message))

            return redirect(url_for('uploaded_file', filename=sheet_path_temp))
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Submit music score PDF</h1>
    <form method=post enctype=multipart/form-data>
        Name: <br>
        <input type=text name=name value="">
        <br>
        Description: <br>
        <input type=text name=description value=""><br>
        URL to Music Score PDF <br>
        <input type=text name=url>
        <input type=submit value=Submit>
    </form>
    '''


@app.route('/tasks', methods=['GET'])
def list_tasks():
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb['tasks_test2']
    myquery = {}
    mydoc = mycol.find(myquery)
    tasks = []
    for x in mydoc:
        task = {
            'id': str(x['_id']),
            'name': x['name'],
            'image_path': CURRENT_SERVER + 'static/' + x['image_path'],
            'xml': x['xml']
        }
        tasks.append(task)
    result = {
        'tasks': tasks
    }
    return jsonify(result)


@app.route('/tasks/<variable>', methods=['GET'])
def get_task(variable):
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb['tasks_test2']
    myquery = {"_id": ObjectId(variable)}
    mydoc = mycol.find_one(myquery)
    result = {
        'id': str(mydoc['_id']),
        'name': mydoc['name'],
        'image_path': CURRENT_SERVER + 'static/' + mydoc['image_path'],
        'xml': mydoc['xml']
    }
    return jsonify(result)


if __name__ == "__main__":
    app.debug = True
    print('in the main')
    context = ('crowdmanager_eu.crt', 'crowdmanager.eu.key')
    app.run(host='0.0.0.0', port=443, ssl_context=context)
