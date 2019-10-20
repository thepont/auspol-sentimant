import {find, client, db} from './database/db';
import {map, toArray, flatMap, reduce, shareReplay, finalize, distinct, first} from 'rxjs/operators'
import  {from} from 'rxjs';
import language from '@google-cloud/language'

const languageClient = new language.LanguageServiceClient();

async function analiseAricles(){
    await client.connect();
    await find({collection: 'people'})({})
        .pipe(
            flatMap(({name}) => find({collection: 'article'})({
                '$text': {'$search': `"${name}"`},
                sentiment : {$exists: false}
            })),
            distinct(ii => ii._id),
            flatMap(async ({content, _id}) => {
                console.log('Requesting Document', content, _id) 
                let document = {
                    content,
                    type: 'PLAIN_TEXT'
                }
                let [result] = await languageClient.analyzeEntitySentiment({document});
                return {
                    _id,
                    sentiment: result
                }
            }),
            flatMap(({_id, sentiment}) => {
               return db().collection('article').updateOne({_id}, {$set: {sentiment}})
            }),
        )
        .toPromise();
    client.close();
}

analiseAricles();

// async function weightedAverage(){
//     await client.connect();
//     await find({collection: 'people'})
//     flatMap(({name}) => async () => {
//         return {
//             name,
//             articleList: find({collection: 'article'})({'sentiment.entities.name': `"${name}"`})
            
//         }
//     ),
//     map(({name, articleList}) => {
//         from(articleList).pipe(
//             flatMap(async (article) => {
//                 return {
//                     sentiment: from(article.sentiment.entities)
//                         .pipe(
//                             filter(ii => ii.name === name),
//                             reduce((average, ii) => {
//                                 average += 
//                             },{avg: 0})
//                         )
//                         .toPromise()
//             })
//         )
//     })
// }


// db.getCollection('article').aggregate([{$match: {"sentiment.entities.name": "Peter Dutton"}}, {$filter: {"sentiment.entities.name": "Peter Dutton"}}])

// db.getCollection('article').aggregate([
//     //   { "$match": { "sentiment.entities.name": "Peter Dutton"} },
//       { "$unwind": "$sentiment.entities" },
//     //   { "$match": { "sentiment.entities": "Peter Dutton"} } ,
//       { "$group": {
//         "_id": "$sentiment.entities.name",
//         "sentiment": { "$push": "$sentiment.entities.sentiment" }
//       }},
//       { "$match": { "_id": "Peter Dutton"} } ,
//     //   { "$group": {
//     //     "_id": "$_id._id",
//     //     "stores": {
//     //       "$push": {
//     //         "_id": "$_id.storeId",
//     //         "offers": "$offers"
//     //       }
//     //     }
//     //   }}
//     ])


// db.getCollection('article').aggregate([
//     { "$unwind": "$sentiment.entities" },
//     { "$group": {
//       "_id": "$sentiment.entities.name",
//       "sentiment": { "$push": { "sentiment": "$sentiment.entities.sentiment", "link": "$link", "type": "$sentiment.entities.type"} },
//       "average":  {"$avg": "$sentiment.entities.sentiment.score"}
//     }},
//     { "$match": { "sentiment.type": "PERSON"}} ,
//     { "$match": {"sentiment.4": {"$exists": true}}},
//     {"$sort": {"average": -1}}
//   ])



// db.getCollection('article').aggregate([
//     { "$unwind": "$sentiment.entities" },
//     { "$group": {
//       "_id": "$sentiment.entities.name",
//       "sentiment": { "$push": { "sentiment": "$sentiment.entities.sentiment", "link": "$link", "type": "$sentiment.entities.type"} },
//       "average":  {"$avg": "$sentiment.entities.sentiment.score"}
//     }},
//     { "$match": { "_id": {"$in": ["Scott Morrison", "Anthony Albanese", "Peter Dutton", "Tanya Plibersek","Josh Frydenberg" ,"Greg Hunt" , "John Howard", "Bridget McKenzie", "Penny Wong", "Mathias Cormann", "Simon Birmingham", "Richard Marles", "Bill Shorten", "Chris Bowen", "Tony Abbott", "Malcolm Turnbull", "Richard Di Natale", " Jacqui Lambie", "Marise Payne", "Michael McCormack", "Linda Reynolds"]} }},
//     { "$match": { "sentiment.type": "PERSON"}},
    
//   //   { "$match": {"sentiment.4": {"$exists": true}}},
//     {"$sort": {"average": 1}}
//   ])



// db.getCollection('article').aggregate([
//     { "$unwind": "$sentiment.entities" },
//   //   { $project: {
//   //         field: {
//   //             $filter: {
//   //                 input: "$sentiment.entities.sentiment"",
//   //                 as: "item",
//   //                 cond: { $gt: [ "$$item.A", 0 ] }
//   //             }
//   //         },
//   //         secondField: "$secondField"
//   //     } 
//   //   }
//     { "$group": {
//       "_id": "$sentiment.entities.name",
//       "sentiment": { "$push": { "sentiment": "$sentiment.entities.sentiment", "link": "$link", "type": "$sentiment.entities.type"} },
//       "average":  {"$avg": "$sentiment.entities.sentiment.score"},
//       "avg": {"$avg": { "$cond":[{
//           "$and": [ {"$gt":[ "$sentiment.entities.sentiment.magnitude", 0]}, {"$ne" :["$sentiment.entities.sentiment.score", 0]}]
//           //,
          
//   //          "$sent.iment.entities.sentiment.score": {"$ne": 0}
//           },
//           "$sentiment.entities.sentiment.score", null]}}
//     }},
//     { "$match": { "_id": {"$in": ["Pauline Hanson", "Scott Morrison", "Anthony Albanese", "Peter Dutton", "Tanya Plibersek","Josh Frydenberg" ,"Greg Hunt" , "John Howard", "Donald Trump", "Jacinta Arden", "Bridget McKenzie", "Penny Wong", "Mathias Cormann", "Simon Birmingham", "Richard Marles", "Bill Shorten", "Chris Bowen", "Tony Abbott", "Malcolm Turnbull", "Richard Di Natale", " Jacqui Lambie", "Marise Payne", "Michael McCormack", "Linda Reynolds"]} }},
//     { "$match": { "sentiment.type": "PERSON"}},
    
//   //   { "$match": {"sentiment.4": {"$exists": true}}},
//     {"$sort": {"avg": 1}}
//   ])