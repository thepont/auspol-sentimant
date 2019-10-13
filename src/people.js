import {fromCSV} from 'rx-from-csv';
import path from 'path'
import {map} from 'rxjs/operators'

const ALL_SEN_PATH = path.resolve(__dirname, '../data/allsenel.csv');
const ALL_REPS_PATH = path.resolve(__dirname, '../data/SurnameRepsCSV.csv');

function allSen(){
    return fromCSV(ALL_SEN_PATH)
}

function allReps(){
    return fromCSV(ALL_REPS_PATH)
}

function convertForDb(record) {
    return {
        name: `${record["First Name"].replace("\\'","'")} ${record["Surname"].replace("\\'","'")}`,
        firstName: record["First Name"].replace("\\'","'"),
        surname: record["Surname"].replace("\\'","'"),
        gender: record["Gender"],
        party: record["Political Party"],
        state: record["LabelState"],
        electorate: (record["Electorate"]).replace("\\'","'")
    }
}

allReps()
    .pipe(
        map(convertForDb)
    )
    .subscribe(ii => {
        console.log(ii);
    })
allSen()
    .pipe(
        map(convertForDb)
    )
    .subscribe(ii => {
        console.log(ii);
    })