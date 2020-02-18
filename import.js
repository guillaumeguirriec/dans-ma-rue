const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run() {
    let anomalies = [];

    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    // Suppression de l'indice
    await client.indices.delete({ index: indexName });

    // Création de l'indice
    await client.indices.create({ index: indexName });

    // Changement du mapping du champ location
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
                date_declaration: formatDateDeclaration(data['ANNEE DECLARATION'], data['MOIS DECLARATION']),
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

            while (anomalies.length) {
                await client.bulk(createBulkInsertQuery(anomalies.splice(0, 20000)));
            }

            await client.close();

            console.log(`Terminated!`);
        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(anomalies) {
    const body = anomalies.reduce((acc, anomalie) => {
        const { annee_declaration, mois_declaration, date_declaration, type, sous_type, code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location } = anomalie;
        acc.push({ index: { _index: indexName, _type: '_doc', _id: anomalie.object_id } });
        acc.push({ '@timestamp': anomalie['@timestamp'], annee_declaration, mois_declaration, date_declaration, type, sous_type, code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location });
        return acc
    }, []);

    return { body };
};

// helper to format date declaration
function formatDateDeclaration(annee, mois) {
    if (mois.length === 1) {
        mois = '0' + mois;
    }
    return `${mois}/${annee}`;
}

run().catch(console.error);
