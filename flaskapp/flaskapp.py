from flask import Flask
from collections import Counter

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello from flask!"

@app.route('/countme/<input_str>')
def count_me(input_str):
    input_counter = Counter(input_str)
    response = []
    for letter, count in input_counter.most_common():
        response.append('"{}": {}'.format(letter, count))
    return '<br>'.join(response)

if __name__ == '__main__':
    app.run()
