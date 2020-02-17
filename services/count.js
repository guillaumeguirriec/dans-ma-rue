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

exports.countAround = async (client, lat, lon, radius, callback) => {
    const response = await client.count({
        index: indexName,
        body: {
            query: {
                bool: {
                    must: {
                        match_all: {}

                    },
                    filter: {
                        geo_distance: {
                            distance: radius,
                            location: `${lat},${lon}`
                        }
                    }
                }
            }
        }
    });
    callback({
        count: response.body.count
    })
}
