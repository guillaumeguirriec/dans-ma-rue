const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run() {
    let anomalies = [];

    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    // Création de l'indice
    await client.indices.create({ index: indexName });

    await client.indices.putMapping({
        index: indexName,
        body: {
            properties: {
                location: {
                    type: "geo_point"
                }
            }
        }
    });

    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            anomalies.push({
                '@timestamp': data.DATEDECL,
                object_id: data.OBJECTID,
                annee_declaration: data['ANNEE DECLARATION'],
                mois_declaration: data['MOIS DECLARATION'],
                type: data.TYPE,
                sous_type: data.SOUSTYPE,
                code_postal: data.CODE_POSTAL,
                ville: data.VILLE,
                arrondissement: data.ARRONDISSEMENT,
                prefixe: data.PREFIXE,
                intervenant: data.INTERVENANT,
                conseil_de_quartier: data['CONSEIL DE QUARTIER'],
                location: data.geo_point_2d,
            });

        })
        .on('end', async () => {

            const slicedAnomalies = chunkArray(anomalies, 20000);

            for (let index = 0; index < slicedAnomalies.length; index++) {
                try {
                    await client.bulk(createBulkInsertQuery(slicedAnomalies[index]));
                } catch (error) {
                    console.error(error);
                }
            }

            await client.close();
            console.log(`Terminated!`);
        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(anomalies) {
    const body = anomalies.reduce((acc, anomalie) => {
        const { annee_declaration, mois_declaration, type, sous_type, code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location } = anomalie;
        acc.push({ index: { _index: indexName, _type: '_doc', _id: anomalie.object_id } });
        acc.push({ '@timestamp': anomalie['@timestamp'], annee_declaration, mois_declaration, type, sous_type, code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location });
        return acc
    }, []);

    return { body };
};

// helper (boilerplate stackoverflow)
function chunkArray(myArray, chunk_size) {
    var index = 0;
    var arrayLength = myArray.length;
    var tempArray = [];

    for (index = 0; index < arrayLength; index += chunk_size) {
        myChunk = myArray.slice(index, index + chunk_size);
        // Do something if you want with the group
        tempArray.push(myChunk);
    }

    return tempArray;
}

run().catch(console.error);
