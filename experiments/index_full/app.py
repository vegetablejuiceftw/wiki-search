import json

from flask import Flask, request

from experiments.index_full.search_opensearch import client, search_query_template, index_name

app = Flask(__name__)

field_boosts = {'aliases-a': 0.6000000000000001, 'aliases-b': 0.6000000000000001, 'aliases-c': 0.45, 'title-a': 0.65, 'title-b': 0.30000000000000004, 'title-c': 0.35000000000000003, 'title-kw': 0.2, 'aliases-kw': 0.9, 'factor-a': 4.694751770347057}


@app.route("/", methods=["GET"])
def main():
    query = request.args.get("query")
    limit = int(request.args.get("limit", "32"))
    if not query:
        return []

    phrases = query.split("|")[:3]
    results = []
    while phrases:
        print(phrases)
        query = search_query_template(phrases, field_boosts)
        # print(json.dumps(query, indent=2))
        try:
            response = client.search(index=index_name, body=query, size=limit * 5, _source=True)
            break
        except:
            phrases.pop(-1)

    hits = response["hits"]["hits"]
    found = set()
    for i, hit in enumerate(hits):
        data = hit['_source']
        if data['wikidata_id'] in found:
            continue
        results.append({
            "score": hit["_score"],
            "wikidata_id": data["wikidata_id"],
            "title": data["title"],
            "aliases": data["aliases"],
            "text": data["text"],
        })
        found.add(data['wikidata_id'])
    return results[:limit]


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=True)
