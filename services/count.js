const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = async (client, from, to, callback) => {
    const response = await client.count({
        index: indexName,
        body: {
            query: {
                range: {
                    "@timestamp": {
                        "gte": from,
                        "lte": to
                    }
                }
            }
        }
    });
    callback({
        count: response.body.count
    })
}

exports.countAround = (client, lat, lon, radius, callback) => {
    // TODO Compter le nombre d'anomalies autour d'un point géographique, dans un rayon donné
    callback({
        count: 0
    })
}