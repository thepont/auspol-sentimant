const MongoClient = require('mongodb').MongoClient;
const {bufferCount, flatMap, finalize, toArray, throwError} = require('rxjs/operators')
const BUFFER_SIZE = 50;
const {Observable, EMPTY, from} = require('rxjs');
const url = "mongodb://localhost";
const dbName = 'auspol';
const client = new MongoClient(url);

const insert = ({collection}) => (obs) => {
    let dbCollection = client.db(dbName).collection(collection)
    return obs.pipe(
        bufferCount(BUFFER_SIZE),
        flatMap(async (items) => {
            return (await dbCollection).insertMany(items)
        })
    );
}



const insertIfNotExist = ({collection, options}) => (obs) => {
    return obs.pipe(
        bufferCount(BUFFER_SIZE),
        flatMap((items) => {
            let findOp = find({collection}, {projection: {_id: 1}});
            let ids = items.map(ii => ii._id);
            return findOp({"_id": {"$in": ids}})
                .pipe(
                    toArray(),
                    flatMap(async (exist) => {
                        let dbCollection = client.db(dbName).collection(collection)
                        let idExist = exist.map(ii => ii._id);
                        let newItems = items.filter(ii => !idExist.includes(ii._id))
                        return dbCollection.insertMany(newItems);
                    }),
                    
                );
        })

    );
}



const find  = ({collection}) => (query = {}, options) => {
    return new Observable(async (subscriber) => {
        let dbCollection = client.db(dbName).collection(collection);
        let cursor = dbCollection.find(query, options);
        cursor.on("data", (document) => {
            console.log('GOT DOCUMENT', document)
            subscriber.next(document)
        })
        cursor.on("end", () => {
            subscriber.complete();
        });
    });
}


module.exports = { 
    client,
    insert,
    find,
    insertIfNotExist
};