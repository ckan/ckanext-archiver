import os
from flask import Flask, request, make_response


def create_app():
    app = Flask(__name__)

    @app.route('/', defaults={"path": ""})
    @app.route('/<path:path>')
    def echo(path):
        status = int(request.args.get('status', 200))

        content = request.args.get('content', '')

        if 'content_long' in request.args:
            content = '*' * 1000001

        response = make_response(content, status)

        headers = [
            item
            for item in list(request.args.items())
            if item[0] not in ('content', 'status')
        ]

        if 'length' in request.args:
            cl = request.args.get('length')
            headers += [('Content-Length', cl)]
        elif content and 'no-content-length' not in request.args:
            headers += [('Content-Length', bytes(len(content)))]

        for k, v in headers:
            response.headers[k] = v

        return response

    @app.route('/WMS_1_3/', defaults={"path": ""})
    @app.route('/WMS_1_3/<path:path>')
    def WMS_1_3(path):
        status = int(request.args.get('status', 200))

        content = request.args.get('content', '')

        if request.args.get('service') == 'WMS':
            if request.args.get('request') == 'GetCapabilities':
                if request.args.get('version') == "1.3":
                    content = get_file_content('wms_getcap_1.3.xml')

        response = make_response(content, status)

        headers = [
            item
            for item in list(request.args.items())
            if item[0] not in ('content', 'status')
        ]

        for k, v in headers:
            response.headers[k] = v

        return response

    @app.route('/WMS_1_1_1/', defaults={"path": ""})
    @app.route('/WMS_1_1_1/<path:path>')
    def WMS_1_1_1(path):
        status = int(request.args.get('status', 200))
        content = request.args.get('content', '')

        if request.args.get('service') == 'WMS':
            if request.args.get('request') == 'GetCapabilities':
                if request.args.get('version') == "1.1.1":
                    content = get_file_content('wms_getcap_1.1.1.xml')

        response = make_response(content, status)

        headers = [
            item
            for item in list(request.args.items())
            if item[0] not in ('content', 'status')
        ]

        for k, v in headers:
            response.headers[k] = v

        return response

    @app.route('/WFS/', defaults={"path": ""})
    @app.route('/WFS/<path:path>')
    def WFS(path):
        status = int(request.args.get('status', 200))
        content = request.args.get('content', '')

        if request.args.get('service') == 'WFS':
            if request.args.get('request') == 'GetCapabilities':
                content = get_file_content('wfs_getcap.xml')

        response = make_response(content, status)

        headers = [
            item
            for item in list(request.args.items())
            if item[0] not in ('content', 'status')
        ]

        for k, v in headers:
            response.headers[k] = v

        return response

    return app


def get_file_content(data_filename):
    filepath = os.path.join(os.path.dirname(__file__), 'data', data_filename)
    assert os.path.exists(filepath), filepath
    with open(filepath, 'rb') as f:
        return f.read()
