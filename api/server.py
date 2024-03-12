from flask import Flask, request, jsonify
import smtplib
import csv
import io
from io import StringIO
from pymongo import MongoClient
import base64
from email.mime.text import MIMEText
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from concurrent.futures import ThreadPoolExecutor, as_completed
count = 0
cluster = MongoClient('mongodb+srv://yakshmahawer:FTGxyMfL2mRpflQD@speedomail.8sm2rqh.mongodb.net/?retryWrites=true&w=majority&appName=SpeedOMail')
db = cluster['test']
collection = db['emails']


def addToDb(fromEmail, csvfile, subject, body, attach, total, failed, resendcsv):
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    data = {
        'userid': '123456',
        'from': fromEmail,
        'to': csvfile,
        'sub': subject,
        'body': body,
        'date': dt_string,
        'attach': attach,
        'total': total,
        'success': total - failed,
        'failed': failed,
        'resend': resendcsv
    }
    collection.insert_one(data)
def inc_count(c):
    global count 
    count = c
app = Flask(__name__)
@app.route("/data")

def data():
    return "I M DEADPOOL"

@app.route("/add", methods=["POST"])
def add():
    user = request.get_json()
    print(user)
    base64_address = user['csvfile']
    smtp_username = user['email']
    smtp_password = user['pass']
    subject = user['sub']
    body = user['body']
    attach = user['attach']
    filename = user['filename']
    fileformat = user['fileformat']
    if attach != '':
        attach_data = attach.split(',')[1]
        decoded_data = base64.b64decode(attach_data)
    not_sent = []
    def send_email(to_email):
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        message = MIMEMultipart()
        message['Subject'] = subject
        message['From'] = smtp_username
        message['To'] = to_email

        message.attach(MIMEText(body, 'plain'))
        if attach != '':                
            attachment = MIMEApplication(decoded_data)
            attachment.add_header('Content-Disposition', 'attachment', filename= filename + '.' + fileformat)
            message.attach(attachment)
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            try:
                server.sendmail(smtp_username, to_email, message.as_string())
            except Exception as e:
                print(to_email)
                not_sent.append(to_email)

    def send_emails_parallel(email_data_list):
        with ThreadPoolExecutor() as executor:
            in_count = 0
            futures = [executor.submit(send_email, **email_data) for email_data in email_data_list]
            for future in as_completed(futures):
                in_count += 1
                inc_count(in_count)
                try:
                    future.result()
                except Exception as e:
                    print(f"Error sending email: {e}")
    email_data_list = []             
    _, base64_data = base64_address.split(";base64,")
    csv_content = base64.b64decode(base64_data).decode("utf-8")
    rows = csv_content.split('\n')
    csv_reader = csv.reader(StringIO(csv_content))
    for row in csv_reader:
        for element in row:
            if element == 'email':
                continue
            data = {'to_email' : element}
            email_data_list.append(data)

    print(len(email_data_list))
    global count 
    count = 0
    send_emails_parallel(email_data_list)

    # Step 1: Create CSV file
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(["Column"])
    for value in not_sent:
        csv_writer.writerow([value])
        # Step 2: Convert CSV to Base64
    csv_content = csv_buffer.getvalue().encode('utf-8')
    base64_data = base64.b64encode(csv_content).decode('utf-8')

    # Step 3: Create Base64 URL
    base64_url = f"data:text/csv;base64,{base64_data}"
    addToDb(smtp_username, base64_address, subject, body, attach, count, len(not_sent), base64_url)
    response_data = {'not_sent': not_sent}
    return jsonify(response_data)


@app.route('/resend', methods = ['POST'])
def resend():
    user = request.get_json()
    resend = user['not_sent']
    smtp_username = user['email']
    smtp_password = user['pass']
    subject = user['sub']
    body = user['body']
    print(user)
    not_sent = []
    def send_email(to_email):
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        message = MIMEMultipart()
        message['Subject'] = subject
        message['From'] = smtp_username
        message['To'] = to_email

        message.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            try:
                server.sendmail(smtp_username, to_email, message.as_string())
            except Exception as e:
                print(to_email)
                not_sent.append(to_email)

    def send_emails_parallel(email_data_list):
        with ThreadPoolExecutor() as executor:
            in_count = 0
            futures = [executor.submit(send_email, **email_data) for email_data in email_data_list]
            for future in as_completed(futures):
                in_count += 1
                inc_count(in_count)
                try:
                    future.result()
                except Exception as e:
                    print(f"Error sending email: {e}")
    email_data_list = []             
    for data in resend:
        data = {'to_email' : data}
        email_data_list.append(data)

    global count 
    count = 0
    send_emails_parallel(email_data_list)
    response_data = {'not_sent': not_sent}
    return jsonify(response_data)


@app.route('/progress', methods = ["GET"])
def progress():
    return str(count)

@app.route('/test', methods = ["GET"])
def test():
    return "Hyya"

@app.route('/login', methods = ["POST"])
def login():
    data = request.get_json()
    result = ''
    if data['code'] == '123456':
        result = "Success"
    else:
        result = "Failed"
    return result
        
if __name__ == '__main__':
    app.run(debug=True)

