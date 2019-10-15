import {fromCSV} from 'rx-from-csv';
import path from 'path';
import {map} from 'rxjs/operators';
import {insertIfNotExist, client} from './database/db';
import uuidv3 from 'uuid/v3';

const PEOPLE_SEED_UUID = 'a7f6c7ed-8d7f-451a-8c39-0069f8525eb7'

const ALL_SEN_PATH = path.resolve(__dirname, '../data/allsenel.csv');
const ALL_REPS_PATH = path.resolve(__dirname, '../data/SurnameRepsCSV.csv');

function allSen(){
    return fromCSV(ALL_SEN_PATH)
}

function allReps(){
    return fromCSV(ALL_REPS_PATH)
}

function convertForDb(record) {
    let preferredName = record["Preferred Name"];
    let firstName = record["First Name"];
    let surname = record["Surname"];
    let electorate = record["Electorate"];
    let name =  `${preferredName || firstName} ${surname}`;
    let _id = uuidv3(`${name}-${electorate}`, PEOPLE_SEED_UUID); 
    return {
        _id,
        name,
        preferredName,
        firstName,
        surname,
        gender: record["Gender"],
        party: record["Political Party"],
        state: record["LabelState"],
        electorate
    }
}

async function ingestPeople(){
    await client.connect();
    await Promise.all([
        allReps()
            .pipe(
                map(convertForDb),
                insertIfNotExist({collection: 'people'})
            )
            .toPromise(),
        allSen()
            .pipe(
                map(convertForDb),
                insertIfNotExist({collection: 'people'})
            )
            .toPromise()]);
    client.close();
}
ingestPeople();