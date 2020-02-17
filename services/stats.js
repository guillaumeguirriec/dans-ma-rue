const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = async (client, callback) => {
    const response = await client.search({
        index: indexName,
        body: {
            size: 0,
            aggs: {
                arrondissements: {
                    terms: {
                        field: "arrondissement.keyword"
                    }
                }
            }
        }
    });
    const stat = response.body.aggregations.arrondissements.buckets.map(element => {
        return {
            "arrondissement": element.key,
            "count": element.doc_count
        };
    });
    callback(stat);
}

exports.statsByType = (client, callback) => {
    // TODO Trouver le top 5 des types et sous types d'anomalies
    callback([]);
}

exports.statsByMonth = (client, callback) => {
    // TODO Trouver le top 10 des mois avec le plus d'anomalies
    callback([]);
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    callback([]);
}
