from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import Elasticsearch
import traceback

app = Flask(__name__)
CORS(app)

es = Elasticsearch("http://183.80.130.239:9200")  # Đổi lại nếu IP thay đổi

@app.route("/")
def home():
    return "Elasticsearch API is running."

@app.route("/query_streams", methods=["POST"])
def query_streams():
    try:
        data = request.json
        playlist = data.get("playlist")
        from_date = data.get("from_date")
        to_date = data.get("to_date")
        single_date = data.get("date")

        if not playlist:
            return jsonify({"error": "Missing playlist parameter"}), 400

        # Trường hợp chỉ truyền 'date', dùng làm cả from & to
        if single_date:
            from_date = to_date = single_date
        elif not (from_date and to_date):
            return jsonify({"error": "Missing date range"}), 400

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"playlistName": playlist}},
                        {
                            "range": {
                                "date": {
                                    "gte": from_date + "T00:00:00",
                                    "lte": to_date + "T23:59:59"
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "total_streams": {
                    "sum": {
                        "field": "stream"
                    }
                }
            }
        }

        result = es.search(index="raw_playlist", body=query)
        total = result["aggregations"]["total_streams"]["value"]
        return jsonify({"total_streams": total})

    except Exception:
        traceback.print_exc()
        return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
