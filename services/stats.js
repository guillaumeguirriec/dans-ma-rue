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
                        field: "arrondissement.keyword",
                        size: 20
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

exports.statsByType = async (client, callback) => {
    const response = await client.search({
        index: indexName,
        body: {
            size: 0,
            aggs: {
                types: {
                    terms: {
                        field: "type.keyword",
                        size: 5
                    },
                    aggs: {
                        sous_types: {
                            terms: {
                                field: "sous_type.keyword",
                                size: 5
                            }
                        }
                    }
                }
            }
        }
    });

    const stat = response.body.aggregations.types.buckets.map(element => {
        return {
            "types": element.key,
            "count": element.doc_count,
            "sous_types": element.sous_types.buckets.map(sous_type => {
                return {
                    "sous_type": sous_type.key,
                    "count": sous_type.doc_count
                }
            })
        };
    });

    callback(stat);
}

exports.statsByMonth = async (client, callback) => {
    const response = await client.search({
        index: indexName,
        body: {
            size: 0,
            aggs: {
                datesdeclaration: {
                    terms: {
                        field: "date_declaration.keyword",
                        size: 10
                    }
                }
            }
        }
    });
    const stat = response.body.aggregations.datesdeclaration.buckets.map(element => {
        return {
            "month": element.key,
            "count": element.doc_count
        };
    });
    callback(stat);
}

exports.statsPropreteByArrondissement = async (client, callback) => {
    const response = await client.search({
        index: indexName,
        body: {
            size: 0,
            query: {
                bool: {
                    must: {
                        match: {
                            type: "Propreté"
                        }
                    }
                }
            },
            aggs: {
                arrondissements: {
                    terms: {
                        field: "arrondissement.keyword",
                        size: 3
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
