from flask import Flask, render_template, g, request
import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError, RqlDriverError
from flask_socketio import SocketIO, emit
from threading import Thread
from subprocess import Popen
import os, os.path
import signal
import time

app = Flask(__name__)
socketio = SocketIO(app)
global thread
thread = None

HOST =  'ec2-34-208-199-46.us-west-2.compute.amazonaws.com'
PORT = 28015
DB = 'test'

# db setup
def dbSetup():
    connection = r.connect(host=HOST, port=PORT)
    try:
        r.db_create(DB).run(connection)
        print 'Database setup completed'
    except RqlRuntimeError:
        print 'Database already exists.'
    finally:
        connection.close()
dbSetup()

# open connection before each request
@app.before_request
def before_request():
    try:
        g.rdb_conn = r.connect(host=HOST, port=PORT, db=DB)
    except RqlDriverError:
        abort(503, "Database connection could be established.")

# close the connection after each request
@app.teardown_request
def teardown_request(exception):
    try:
        g.rdb_conn.close()
    except AttributeError:
        pass

@app.route('/', methods=['GET', 'POST'])
def hello():
    if request.method == 'POST':
      if 'StartIngestion' in request.form:
        print "Start Ingestion clicked with " + str(request.form['numberOfProducers']) + " producers"
        p = Popen('python k-consumer.py', shell=True)
        processId1 = p.pid
        time.sleep(1)
        p = Popen('python k-producer.py ' + request.form['numberOfProducers'], shell=True)
        processId2 = p.pid
        file = open("pid.txt","w")
        file.write(processId1 + "\n")
        file.write(processId2)
        file.close()
      if 'StopIngestion' in request.form:
        print "Stop Ingestion clicked"
        if os.path.isfile("pid.txt"):
          file = open("pid.txt","r")
          for line in file:
            os.kill(int(line), signal.SIGTERM)

    return render_template('index.html')

@app.route('/refund')
def refund():
    cursor = r.table("REFUND").run(g.rdb_conn)
    stats = []
    stats.append(r.table("REFUND").count().run(g.rdb_conn))
    stats.append(0)
    if stats[0]>0:
      avg = float("%.3f" % r.table("REFUND").avg("timestamp").run(g.rdb_conn))
      stats[1] = int(avg*1000)
    posts = []
    for document in cursor:
        posts.append(document)
    return render_template('REFUND.html', posts=posts, stats=stats)

@app.route('/gift_card')
def gift_card():
    cursor = r.table("GIFT_CARD").run(g.rdb_conn)
    stats = []
    stats.append(r.table("GIFT_CARD").count().run(g.rdb_conn))
    stats.append(0)
    if stats[0]>0:
      avg = float("%.3f" % r.table("GIFT_CARD").avg("timestamp").run(g.rdb_conn))
      stats[1] = int(avg*1000)
    posts = []
    for document in cursor:
        posts.append(document)
    return render_template('GIFT_CARD.html', posts=posts, stats=stats)

@app.route('/order')
def order():
    cursor = r.table("ORDER").run(g.rdb_conn)
    stats = []
    stats.append(r.table("ORDER").count().run(g.rdb_conn))
    stats.append(0)
    if stats[0]>0:
      avg = float("%.3f" % r.table("ORDER").avg("timestamp").run(g.rdb_conn))
      stats[1] = int(avg*1000)
    posts = []
    for document in cursor:
        posts.append(document)
    return render_template('ORDER.html', posts=posts, stats=stats)

@app.route('/payment')
def payment():
    cursor = r.table("PAYMENT").run(g.rdb_conn)
    stats = []
    stats.append(r.table("PAYMENT").count().run(g.rdb_conn))
    stats.append(0)
    if stats[0]>0:
      avg = float("%.3f" % r.table("PAYMENT").avg("timestamp").run(g.rdb_conn))
      stats[1] = int(avg*1000)
    posts = []
    for document in cursor:
        posts.append(document)
    return render_template('PAYMENT.html', posts=posts, stats=stats)

@app.route('/deals')
def deals():
    cursor = r.table("DEALS").run(g.rdb_conn)
    stats = []
    stats.append(r.table("DEALS").count().run(g.rdb_conn))
    stats.append(0)
    if stats[0]>0:
      avg = float("%.3f" % r.table("DEALS").avg("timestamp").run(g.rdb_conn))
      stats[1] = int(avg*1000)
    posts = []
    for document in cursor:
        posts.append(document)
    return render_template('DEALS.html', posts=posts, stats=stats)

def watch_messages_refund():
    rdb_conn = r.connect(host=HOST, port=PORT, db=DB)
    feed = r.table("REFUND").changes().run(rdb_conn)
    for update in feed:
        new_message=update['new_val']
        new_message['new_count'] = r.table("REFUND").count().run(rdb_conn)
        new_message['new_avg'] = int(float("%.3f" % r.table("REFUND").avg("timestamp").run(rdb_conn))*1000)
        socketio.emit('new_message_refund', new_message)

def watch_messages_deals():
    rdb_conn = r.connect(host=HOST, port=PORT, db=DB)
    feed = r.table("DEALS").changes().run(rdb_conn)
    for update in feed:
        new_message=update['new_val']
        new_message['new_count'] = r.table("DEALS").count().run(rdb_conn)
        new_message['new_avg'] = int(float("%.3f" % r.table("DEALS").avg("timestamp").run(rdb_conn))*1000)
        socketio.emit('new_message_deals', new_message)

def watch_messages_order():
    rdb_conn = r.connect(host=HOST, port=PORT, db=DB)
    feed = r.table("ORDER").changes().run(rdb_conn)
    for update in feed:
        new_message=update['new_val']
        new_message['new_count'] = r.table("ORDER").count().run(rdb_conn)
        new_message['new_avg'] = int(float("%.3f" % r.table("ORDER").avg("timestamp").run(rdb_conn))*1000)
        socketio.emit('new_message_order', new_message)

def watch_messages_payment():
    rdb_conn = r.connect(host=HOST, port=PORT, db=DB)
    feed = r.table("PAYMENT").changes().run(rdb_conn)
    for update in feed:
        new_message=update['new_val']
        new_message['new_count'] = r.table("PAYMENT").count().run(rdb_conn)
        new_message['new_avg'] = int(float("%.3f" % r.table("PAYMENT").avg("timestamp").run(rdb_conn))*1000)
        socketio.emit('new_message_payment', new_message)

def watch_messages_gift_card():
    rdb_conn = r.connect(host=HOST, port=PORT, db=DB)
    feed = r.table("GIFT_CARD").changes().run(rdb_conn)
    for update in feed:
        new_message=update['new_val']
        new_message['new_count'] = r.table("GIFT_CARD").count().run(rdb_conn)
        new_message['new_avg'] = int(float("%.3f" % r.table("GIFT_CARD").avg("timestamp").run(rdb_conn))*1000)
        socketio.emit('new_message_gift_card', new_message)

if __name__ == '__main__':
    if thread is None:
        # app.debug = True
        thread1 = Thread(target=watch_messages_refund)
        thread1.start()
        thread2 = Thread(target=watch_messages_deals)
        thread2.start()
        thread3 = Thread(target=watch_messages_order)
        thread3.start()
        thread4 = Thread(target=watch_messages_payment)
        thread4.start()
        thread5 = Thread(target=watch_messages_gift_card)
        thread5.start()
    socketio.run(app)
