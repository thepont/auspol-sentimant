const MongoClient = require('mongodb').MongoClient;
const {bufferCount, flatMap, finalize, toArray, throwError} = require('rxjs/operators')
const BUFFER_SIZE = 50;
const {Observable, EMPTY, from} = require('rxjs');
const url = "mongodb://localhost";
const dbName = 'auspol';
const client = new MongoClient(url);

const insert = ({collection}) => (obs) => {
    let dbCollection = client.connect().then(() => {
        return client.db(dbName).collection(collection)
    });
    return obs.pipe(
        bufferCount(BUFFER_SIZE),
        flatMap(async (items) => {
            return (await dbCollection).insertMany(items)
        }),
        finalize(() => {
            client.close();
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
                    flatMap((exist) => {
                        // console.log('here')
                        return from(client.connect().then(() => {
                                console.log('Connected Insert')
                                return client.db(dbName).collection(collection)
                            }))
                            .pipe(
                                flatMap(async (collection) => {
                                    console.log('got connection insert')
                                    let idExist = exist.map(ii => ii._id);
                                    let newItems = items.filter(ii => !idExist.includes(ii._id))
                                    try {
                                        let ret  = await collection.insertMany(newItems);
                                        // client.close();
                                        return ret;
                                    } catch (ee){
                                        // client.close();
                                        throw ee
                                    }
                                    // client.close();
                                    
                                    // return newItems ? await collection.insertMany(newItems).then(() => client.close()) : Promise.resolve([]);
                                }),
                                finalize(() => {
                                    console.log('insert if not exist, close')
                                    // client.close();
                                })
                            );
                       
                        // try{

                        //     let dbCollection = client.connect().then(() => {
                        //         return client.db(dbName).collection(collection)
                        //     });
                        //     let idExist = exist.map(ii => ii._id);
                        //     let newItems = items.filter(ii => !idExist.includes(ii._id))
                        //     let result = await (await dbCollection).insertMany(newItems);
                        //     client.close();
                        //     return result;
                        // } catch (ee){
                        //     client.close();
                        //     throw ee;
                        // }
                    }),
                    
                );
        })

    );
}



const find  = ({collection}) => (query = {}, options) => {
    return new Observable(async (subscriber) => {
        console.log('Opening Find')
        let dbCollection = await client.connect().then(() => {
            return client.db(dbName).collection(collection)
        });
        let cursor = dbCollection.find(query, options);
        cursor.on("data", (document) => {
            subscriber.next(document)
        })
        cursor.on("end", () => {
            console.log('find, close')
            subscriber.complete();
            // client.close();
        });
    });
}


module.exports = { 
    client,
    insert,
    find,
    insertIfNotExist
};