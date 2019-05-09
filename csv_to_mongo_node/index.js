const csv = require('csvtojson');

const MongoClient = require('mongodb').MongoClient;

const url = 'mongodb://localhost:27017';
const dbName = 'myproject';

MongoClient.connect(url, function(err, client) {
    console.log("Connected successfully to server");

    const db = client.db(dbName);
    insertDocuments(db, ()=>{
        client.close();
    })
});

const insertDocuments = (db, callback) => {
    let collection = db.collection('documents');
    const csvFilePath = '/home/garvit/meta.csv';

    csv()
        .fromFile(csvFilePath)
        .then((jsonObj)=>{
            collection.insertMany(jsonObj, function(err, result) {
                console.log("result" , result);
                console.log("error", err);
                callback(result);
            });
        })
}
