from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import Elasticsearch
import traceback
import logging
import sys

# Setup logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# Flask app setup
app = Flask(__name__)
CORS(app)

# Kết nối tới Elasticsearch (đổi IP nếu cần)
es = Elasticsearch("http://183.80.130.239:9200")

@app.route("/")
def home():
    return "Elasticsearch API is running."

@app.route("/query_streams", methods=["POST"])
def query_streams():
    try:
        data = request.json
        playlist = data.get("playlist")
        from_date = data.get("from_date") or data.get("date")
to_date = data.get("to_date") or data.get("date")


        if not all([playlist, from_date, to_date]):
            return jsonify({"error": "Missing parameters"}), 400

        # Truy vấn Elasticsearch
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"playlistName": playlist}},
                        {
                            "range": {
                                "date": {
                                    "gte": f"{from_date}T00:00:00",
                                    "lte": f"{to_date}T23:59:59"
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

        logging.debug(f"Querying ES with body: {query}")
        result = es.search(index="raw_playlist", body=query)
        total = result["aggregations"]["total_streams"]["value"]
        return jsonify({"total_streams": total})

    except Exception as e:
        logging.exception("Error during Elasticsearch query")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
