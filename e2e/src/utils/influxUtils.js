// TBD - leverage influxdbv2 REST API
const process = require('process');
const axios = require('axios');
const fs = require('fs');
const csvParseSync = require('csv-parse/lib/sync');

let active_config = require(__basedir + '/e2e.conf.json').active;
//let config = require(__basedir + '/e2e.conf.json')[active_config];
let setHeadless = false;
let newHeadless = false;
let selDocker = false;

const mil2Nano = 1000000;

process.argv.slice(2).forEach((val) => {

    let pair = val.split('=');

    switch(pair[0]){
    case 'headless': //overrides value in config file
        //config.headless = (pair[1] === 'true');
        setHeadless = true;
        newHeadless = (pair[1] === 'true');
        break;
    case 'sel_docker':
    case 'selDocker':
        //config.sel_docker = (pair[1] === 'true');
        selDocker = (pair[1] === 'true');
        break;
    case 'activeConf':
    case 'active_conf':
        //config = require(__basedir + '/e2e.conf.json')[pair[1]];
        active_config = pair[1];
    }
});

const config = require(__basedir + '/e2e.conf.json')[active_config];
const defaultUser = config.default_user;
config.sel_docker = selDocker;
config.headless = setHeadless ? newHeadless : config.headless;

global.__config = config;
global.__defaultUser = defaultUser;
global.__users = { 'init': undefined };
global.__killLiveDataGen = false;
global.__liveDataGenRunning = false;

console.log(config.headless ? 'running headless' : 'running headed');
console.log(config.sel_docker ? 'running for selenium in docker' : 'running for selenium standard');
console.log(`active configuration ${JSON.stringify(config)}`);

axios.defaults.baseURL = `${config.protocol}://${config.host}:${config.port}`;

/* Uncomment to debug axios
axios.interceptors.request.use(request => {
    console.log('Starting Request', request)
    return request
})

axios.interceptors.response.use(response => {
    console.log('Response:', response)
    return response
})
*/

const flush = async () => {
    // console.log('calling flush')
    await axios.get('/debug/flush');
    delete global.__users;
    global.__users = { 'init': undefined };
};

// user { username: 'name', password: 'password', org; 'orgname', bucket: 'bucketname' }
const setupUser = async(user) => {
    await axios.post('/api/v2/setup', user).then(resp => {
        user.id = resp.data.user.id;
        user.orgid = resp.data.org.id;
        user.bucketid = resp.data.bucket.id;
        putUser(user);
        //console.log("DEBUG __users: " + JSON.stringify(__users, null, '\t'))
        //console.log("DEBUG resp " + JSON.stringify(resp.data, null, '\t'))
    });
};

// user { username: 'name', password: 'password', org; 'orgname', bucket: 'bucketname' }
const putUser = (user) => {
    if(!(user.username in __users)){
        __users[user.username] = user;
        return;
    }
    throw `${user.username} already defined in global users`;
};

const getUser = (name) => {

    if(name.toUpperCase() === 'DEFAULT'){
        return __users[__defaultUser.username];
    }

    if(name in __users){
        return __users[name];
    }
    throw `"${name}" is not a key in global users`;
};

const signIn = async (username) => {

    let user = getUser(username);
    return await axios.post('/api/v2/signin', '', {auth: {username: user.username, password: user.password}}).then(async resp => {
        //console.log("DEBUG Headers " + JSON.stringify(resp.headers))
        let cookie = resp.headers['set-cookie'][0];
        axios.defaults.headers.common['Cookie'] = cookie; //for axios
        //let pair = cookie.split('=')
        //let expiry = new Date(Date.now() + (24 * 60 * 60 * 1000));
        //await __wdriver.get(`${__config.protocol}://${__config.host}:${__config.port}/`)  //  not working even with valid session cookie redirected to login
        //await __wdriver.manage().addCookie({name: pair[0], value: pair[1], expiry: expiry, domain: __config.host}); //for selenium
        return cookie;
    }).catch(err => {
        console.log(err);
        throw(err); //need to rethrow error - otherwise cucumber will not catch it and test will be success
    });

};

const endSession = async() => {
    delete axios.defaults.headers.common['Cookie'];
};

let nowNano = new Date().getTime() * 1000000;

let intervalNano = 600 * 1000 * 1000000; //10 min in nanosecs

const writeData = async (org, //string
    bucket, //string
    lines = ['testmeas value=300 ' + (nowNano - (3 * intervalNano)),
        'testmeas value=200 ' + (nowNano - (2 * intervalNano)),
        'testmeas value=100 ' + (nowNano - intervalNano)],
    chunkSize = 100) => {

    let chunk = [];
    let chunkCt = 0;

    while(chunkCt < lines.length){
        chunk = ((chunkCt + chunkSize) <= lines.length) ?
            lines.slice(chunkCt, chunkCt + chunkSize - 1) :
            lines.slice(chunkCt, chunkCt + (lines.length % chunkSize));
        await axios.post('/api/v2/write?org=' + org + '&bucket=' + bucket, chunk.join('\n') ).then(() => {
            //  console.log(resp.status)
        }).catch( err => {
            console.log(err);
        });

        chunkCt += chunkSize;
        chunk = [];
    }

};

const query = async(orgID, //string
    query // string
) => {
    return await axios({method: 'post',
        url: '/api/v2/query?orgID=' + orgID,
        data: {'query': query} }).then(async response => {
        return response.data;
    }).catch(async err => {
        console.log('CAUGHT ERROR: ' + err);
    });


};

//Parse query result string to Array[Map()] of column items
const parseQueryResults = async(results) => {
    let resultsArr = results.split('\r\n,');

    let resultsMapArr = [];

    let resultsMapKeys = resultsArr[0].split(',');

    resultsMapKeys.shift(); //first element is empty string

    for(let i = 0; i < resultsArr.length; i++){
        if(i === 0){
            continue;
        }
        resultsMapArr[i-1] = new Map();
        let colVals = resultsArr[i].split(',');
        for(let j = 0; j < resultsMapKeys.length; j++){
            await resultsMapArr[i-1].set(resultsMapKeys[j], colVals[j]);
        }
    }

    return resultsMapArr;
};

//{"name":"ASDF","retentionRules":[],"orgID":"727d19908f30184f","organization":"qa"}
const createBucket = async(orgId, // String
    orgName, //String
    bucketName, //String
) => {
    //throw "createBuctket() not implemented";
    return await axios({
        method: 'post',
        url: '/api/v2/buckets',
        data: { 'name': bucketName, 'orgID': orgId, 'organization': orgName }
    }).then(async response => {
        return response.data;
    }).catch(async err => {
        console.log('influxUtils.createBucket - Error ' + err);
    });
};

//TODO - create cell and view to attach to dashboard
const createDashboard = async(name, orgId) => {
    return await axios.post('/api/v2/dashboards', { name, orgId }).then(resp => {
        return resp.data;
    }).catch(err => {
        console.log('ERROR: ' + err);
        throw(err); // rethrow it to cucumber
    });
};

const getDashboards = async() => {
    return await axios.get('/api/v2/dashboards').then(resp => {
        return resp.data;
    }).catch(err => {
        console.log('ERROR: ' + err);
        throw(err);
    });
};

// http://localhost:9999/api/v2/labels
// {"orgID":"8576cb897e0b4ce9","name":"MyLabel","properties":{"description":"","color":"#7CE490"}}
const createLabel = async(orgId,
    labelName,
    labelDescr,
    labelColor ) =>{

    return await axios({
        method: 'post',
        url: '/api/v2/labels',
        data: { 'orgId': orgId, 'name': labelName,
            'properties': { 'description': labelDescr, 'color': labelColor }}
    }).then(resp => {
        return resp.data;
    }).catch(err => {
        console.log('ERROR: ' + err);
        throw(err);
    });
};


const createVariable = async(orgId, name, type, values, selected = null ) => {

    let parseValues = JSON.parse(values);

    let reSel = selected === null ? selected : JSON.parse(selected);

    return await axios({
        method: 'post',
        url: '/api/v2/variables',
        data: {
            'orgId': orgId,
            'name': name,
            'selected': reSel,
            'arguments': {
                'type': type,
                'values': parseValues
            }
        }
    }).then(resp => {
        return resp.data;
    }).catch(err => {
        console.log('ERROR: ' + err );
        throw(err);
    })

};

const getDocTemplates = async(orgId) => {

    return await axios({
        method: 'get',
        url: `/api/v2/documents/templates?orgID=${orgId}`
    }).then(resp => {
        return resp.data;
    }).catch(err => {
        throw(err);
    });

};

const createTemplateFromFile = async(filepath, orgID) => {
    let content = await readFileToBuffer(process.cwd() + '/' + filepath);
    let newTemplate = JSON.parse(content);
    newTemplate.orgID = orgID;

    return await axios({
        method: 'POST',
        url: '/api/v2/documents/templates',
        data: newTemplate
    }).then(resp => {
        return resp.data;
    }).catch(err => {
        throw(err);
    });

};

const createAlertCheckFromFile = async(filepath, orgID) => {

    let content = await readFileToBuffer(process.cwd() + '/' + filepath);
    //let re = /\\/g;
    //content = content.replace(/\\/g, "\\\\\\");
    ///console.log("DEBUG content \n" + content +  "\n");

    //let newCheck = JSON.parse('{"id":null,"type":"threshold","status":"active","activeStatus":"active","name":"ASDF","query":{"name":"","text":"from(bucket: \\"qa\\")\\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\\n  |> filter(fn: (r) => r[\\"_measurement\\"] == \\"test\\")\\n  |> filter(fn: (r) => r[\\"_field\\"] == \\"val\\")\\n  |> aggregateWindow(every: 1m, fn: mean)\\n  |> yield(name: \\"mean\\")","editMode":"builder","builderConfig":{"buckets":["qa"],"tags":[{"key":"_measurement","values":["test"],"aggregateFunctionType":"filter"},{"key":"_field","values":["val"],"aggregateFunctionType":"filter"},{"key":"gen","values":[],"aggregateFunctionType":"filter"}],"functions":[{"name":"mean"}],"aggregateWindow":{"period":"1m"}},"hidden":false},"orgID":"05a6a2d5ea213000","labels":[],"every":"1m","offset":"0s","statusMessageTemplate":"Check: ${ r._check_name } is: ${ r._level }","tags":[],"thresholds":[{"type":"greater","value":7.5,"level":"CRIT"}]}');
    let newCheck = JSON.parse(content);

    newCheck.orgID = orgID;

    return await axios({
        method: 'POST',
        url: '/api/v2/checks',
        data: newCheck
    }).then(resp => {
        return resp.data;
    }).catch(err => {
        console.error("DEBUG err " + JSON.stringify(err));
        throw(err);
    });

};

const writeLineProtocolData = async (user, def) => {

    let define = JSON.parse(def);

    let dataPoints = [];
    let nowMillis = new Date().getTime();
    //line protocol i.e. myMeasurement,host=myHost testField="testData" 1556896326
    let intervals = await getIntervalMillis(define.points, define.start);
    let startMillis = nowMillis - intervals.full;

//    let samples = await genPoints(define.algo, define.points);
    let samples;
    if(define.data === undefined) {
        samples = await genPoints(define.algo, define.points);
    }else{
        samples = await genPoints(define.algo, define.points, define.data)
    }

    if(define.algo === 'dico'){
        for (let i = 0; i < samples.length; i++) {
            dataPoints.push(`${define.name},test=generic ${define.measurement}="${samples[i]}" ${(startMillis + (intervals.step * i)) * mil2Nano}\n`);
        }
    }else {
        for (let i = 0; i < samples.length; i++) {
            dataPoints.push(`${define.name},test=generic ${define.measurement}=${samples[i]} ${(startMillis + (intervals.step * i)) * mil2Nano}\n`);
        }
    }

    await writeData(user.org, user.bucket, dataPoints);
};

// sample def : { "points": 10, "measurement":"level", "start": "-60h", "algo": "hydro", "prec": "sec"}
const genLineProtocolFile = async(filePath, def) => {
    let define = JSON.parse(def);

    if(fs.existsSync(filePath)) {
        console.log('Removing pre-existing file ' + filePath);
        await fs.unlink(filePath, async err => {
            if (err) {
                console.log('Failed to remove file ' + filePath);
            }
        });
    }

    let dataPoints = [];
    let nowMillis = new Date().getTime();
    //line protocol i.e. myMeasurement,host=myHost testField="testData" 1556896326
    let intervals = await getIntervalMillis(define.points, define.start);
    let startMillis = nowMillis - intervals.full;

    let samples
    if(define.data === undefined) {
        samples = await genPoints(define.algo, define.points);
    }else{
        samples = await genPoints(define.algo, define.points, define.data)
    }

    for(let i = 0; i < samples.length; i++){
        dataPoints.push(`${define.name},test=generic ${define.measurement}=${samples[i]} ${startMillis + (intervals.step * i)}\n`);
    }

    await dataPoints.forEach(async point => {
        await fs.appendFile(filePath, point, err => {
            if(err){console.log('Error writing point ' + point + ' to file ' + filePath);}
        });
    });

};

const genPoints = async (algo, count, data = null) => {
    let samples = [];
    switch(algo.toLowerCase()){
    case 'fibonacci':
        samples = await genFibonacciValues(count);
        break;
    case 'hydro':
        samples = await genHydroValues(count);
        break;
    case 'sine':
        samples = await genSineValues(count);
        break;
    case 'log':
        samples = await genLogisticValues(count);
        break;
    case 'life':
        samples = await genLifeValues(count);
        break;
    case 'dico':
        samples = await genDicoValues(count,data);
        break;
    default:
        throw `Unhandled mode ${algo}`;
    }
    return samples;
};

// 'start' should have time format e.g. -2h, 30m, 1d
const getIntervalMillis = async(count, start) => {
    let time = start.slice(0, -1);
    let fullInterval  = 0;
    let pointInterval = 0;
    switch(start[start.length - 1]){
    case 'd': //days
        fullInterval = Math.abs(parseInt(time)) * 24 * 60000 * 60;
        break;
    case 'h': //hours
        fullInterval = Math.abs(parseInt(time)) * 60000 * 60;
        break;
    case 'm': //minutes
        fullInterval = Math.abs(parseInt(time)) * 60000;
        break;
    case 's': //seconds
        fullInterval = Math.abs(parseInt(time)) * 1000;
        break;
    default:
        throw new `unhandle time unit ${start}`;
    }

    pointInterval = fullInterval / count;
    return {full: fullInterval, step: pointInterval};
};

const genFibonacciValues = async (count) => {
    let result = [];
    for(let i = 0; i < count; i++){
        if(i === 0){
            result.push(1);
        }else if(i === 1){
            result.push(2);
        }else{
            result.push(result[i-1] + result[i-2]);
        }
    }
    return result;
};

const genHydroValues = async(count) => {
    let current = Math.floor(Math.random() * 400) + 100;
    let result = [];
    let trend = (Math.floor(Math.random() * 10) - 5);

    for(let i = 0; i < count; i++){
        current += (Math.floor(Math.random() * 10) + trend);
        result.push(current/100.0);
        if(current < trend){ //negative trend could lead to neg value so set new trend
            trend = Math.floor(Math.random() * 5);
        }else if(current > (500 - trend)){ // pushing ceiling set neg trend
            trend = Math.floor(Math.random() * -5);
        }
    }
    return result;
};

const genSineValues = async(count) => {
    let result = [];
    for(let i = 0; i < count; i++){
        result.push(Math.sin(i) * 1000);
    }
    return result;
};

const genLogisticValues = async(count) => {
    let result = [];
    let reset = ((Math.random() * 5) * 10) + 10;
    let resetReset = Math.round(reset);
    for(let i = 0; i < count; i++){
        result.push(((1/(1 + ((1/2.71828)**(i % reset)))) * 100) + ((Math.random() * 10) - 5)); // + ((Math.random() * 10) - 5));
        if(i === resetReset){
            reset = ((Math.random() * 5) * 10) + 10;
            resetReset += Math.round(reset);
        }
    }
    return result;
};

const genLifeValues = async(count) => {
    let result = [];
    let carryCap = ((Math.random() * 9) * 100) + 100;
    let growthRate = Math.random() + 1;
    //console.log("DEBUG carryCap " + carryCap + " growthRate " + growthRate);
    result.push(2.0);
    for(let i = 1; i < count; i++){
        let accGrowth = growthRate + (0.10 - (Math.random() * 0.20));
        if(result[i-1] > carryCap){ //cull
            let overshoot = result[i -1] - carryCap;
            let cull = overshoot * 2; //((Math.random() * 2) + 1);
            //console.log('DEBUG culling ' + cull  + "  cull percent " + cull/carryCap);
            if(carryCap - cull < 2){
                result.push(2); //reseed
                growthRate = growthRate * 0.95;
            }else{
                result.push(result[i - 1] - cull);
            }
            let origCarryCap = carryCap;
            carryCap -= (carryCap * (cull/carryCap * 0.1)); //overshoot degrades carrying capacity
            if(carryCap < origCarryCap / 2){
                carryCap = origCarryCap/2;
            }
        }else if(result[i-1] < 2){
            result.push(2);
        }else{
            result.push(result[i - 1] ** accGrowth);
        }
    }
    return result;
};

const genDicoValues = async(count,data) => {
    let result = [];
    for(let i = 0; i < count; i++){
        result[i] = data[Math.floor(Math.random() * Math.floor(data.length))];
    }
    return result;
};


const readFileToBuffer = async function(filepath) {
    return await fs.readFileSync(filepath, 'utf-8'); //, async (err) => {
};

const removeFileIfExists = async function(filepath){
    if(fs.existsSync(filepath)) {
        fs.unlinkSync(filepath);
    }
};

const removeFilesByRegex = async function(regex){
  let re = new RegExp(regex)
  await fs.readdir('.', (err, files) => {
        for(var i = 0; i < files.length; i++){
            var match = files[i].match(re);
            if(match !== null){
                fs.unlinkSync(match[0]);
            }
        }
    });
};

const fileExists = async function(filePath){
    return fs.existsSync(filePath);
};

const verifyFileMatchingRegexFilesExist = async function(regex, callback){
    let re = new RegExp(regex);
    let files = fs.readdirSync('.');

    for(var i = 0; i < files.length; i++){
        var match = files[i].match(re);
        if(match !== null){
            return true;
        }
    }

    return false;
};

const waitForFileToExist = async function(filePath, timeout = 60000){
    let sleepTime = 3000;
    let totalSleep = 0;
    while (totalSleep < timeout){
        if(fs.existsSync(filePath)){
            return true;
        }
        await __wdriver.sleep(sleepTime);
        totalSleep += sleepTime;
    }

    throw `Timed out ${timeout}ms waiting for file ${filePath}`;

};

const getNthFileFromRegex = async function(fileregex, index){
    let re = await new RegExp(fileregex);
    let files = fs.readdirSync('.');
    let matchFiles = [];

    for(var i = 0; i < files.length; i++){
        var match = files[i].match(re);
        if(match !== null){
            matchFiles.push(files[i]);
        }
    }

    return matchFiles[index - 1];
};

const readCSV = async function(content){
    return await csvParseSync(content, { columns: true, skip_empty_lines: true, comment: "#"});
};


function sleep(ms){
    return new Promise(resolve => setTimeout(() => resolve(), ms));
}

const dataGenProcess = async function(def = {pulse: 333, model: 'count10'}){

   let total = 100;
   let point = -1;
   let val;
   while(point++ < total && !__killLiveDataGen) {
       let current = (new Date()).getTime();
       switch(def.model){
           case 'count10':
               val = point%10;
               break;
           default:
               val = `"model_${def.model}_undefined"`;
               break;
       }
       //console.log("PULSE " + val);
       await writeData(__defaultUser.org,__defaultUser.bucket, [
           `test,gen=gen val=${val} ${current * mil2Nano}`
       ]);
       __liveDataGenRunning = true;
       await sleep(def.pulse)
   }

   console.warn(`Live data generation process stopped ${(new Date()).toISOString()}`);
    __liveDataGenRunning = false;
};

const startLiveDataGen = function(def){
    if(!__liveDataGenRunning) {
        console.log(`Starting live generator with ${JSON.stringify(def)} ${(new Date()).toISOString()}`);
        __killLiveDataGen = false;
        dataGenProcess(JSON.parse(def));
    }else{
        console.log(`Live Data Generator already running ${(new Date()).toISOString()}`);
    }
};

const stopLiveDataGen = function(){
   __killLiveDataGen = true;
};

module.exports = { flush,
    config,
    defaultUser,
    setupUser,
    putUser,
    getUser,
    signIn,
    endSession,
    writeData,
    createDashboard,
    createVariable,
    getDashboards,
    query,
    createBucket,
    parseQueryResults,
    createLabel,
    createAlertCheckFromFile,
    genLineProtocolFile,
    getIntervalMillis,
    getDocTemplates,
    getNthFileFromRegex,
    genFibonacciValues,
    writeLineProtocolData,
    readFileToBuffer,
    readCSV,
    createTemplateFromFile,
    removeFileIfExists,
    removeFilesByRegex,
    startLiveDataGen,
    stopLiveDataGen,
    fileExists,
    verifyFileMatchingRegexFilesExist,
    waitForFileToExist
};

//flush()
