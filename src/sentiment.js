import {find, client} from './database/db';
import {map, toArray, flatMap, reduce, shareReplay, finalize} from 'rxjs/operators'

async function analiseAricles(){
    await client.connect();
    await find({collection: 'people'})({})
        .pipe(
            flatMap(({name}) => find({collection: 'article'})({'$text': {'$search': `"${name}"`}}))
        )
        .toPromise();
    client.close();
}

analiseAricles();