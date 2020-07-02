// TBD - leverage influxdbv2 REST API
const process = require('process');
const axios = require('axios');
const fs = require('fs');
const csvParseSync = require('csv-parse/lib/sync');
const {exec} = require('child_process');
const util = require('util');
const exec_prom = util.promisify(exec);

let active_config = require(__basedir + '/e2e.conf.json').active;
//let config = require(__basedir + '/e2e.conf.json')[active_config];
let setHeadless = false;
let newHeadless = false;
let selDocker = false;

let nowNano = new Date().getTime() * 1000000;
let intervalNano = 600 * 1000 * 1000000; //10 min in nanosecs

const {InfluxDB} = require('@influxdata/influxdb-client');
const { AuthorizationsAPI,
    BucketsAPI,
    ChecksAPI,
    DashboardsAPI,
    DocumentsAPI,
    LabelsAPI,
    OrgsAPI,
    SetupAPI,
    VariablesAPI} = require('@influxdata/influxdb-client-apis');


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
global.__reportedResetOnce = false;
global.__dockerRun = false;

if(typeof __config.download_dir === 'undefined'){
    __config.download_dir = __basedir;
}

console.log(config.headless ? 'running headless' : 'running headed');
console.log(config.sel_docker ? 'running for selenium in docker' : 'running for selenium standard');
console.log(`active configuration ${JSON.stringify(config)}`);

__config.download_dir = __config.download_dir + '/etc/';
/*
if(typeof process.env['HOSTNAME'] !== 'undefined' && process.env['HOSTNAME'].match(/^[a-f0-9]{12}/)){
    console.log("MATCHED docker style hostname");
    __dockerRun = true;
    __config.download_dir = __config.download_dir + '/etc/';
}
*/
//redefine any config fields based on ENV Properties of the form E2E_<ACTIVE_CONF>_<FIELD_KEY>
/*
Object.keys(__config).forEach(k => {
    let envar = `E2E_${active_config.toUpperCase()}_${k.toUpperCase()}`;
    console.log(`${envar}=${process.env[envar]} typeof ${typeof __config[k]}`);
    if(typeof process.env[envar] !== 'undefined'){
        console.log('DEBUG redefining var ' + process.env[envar]);
        __config[k] = process.env[envar];
    }
});
*/

const resetConfigFieldsToEnvar = (base, o) => {
//    console.log(`DEBUG o ${JSON.stringify(o)}`);
    Object.keys(o).forEach( k => {
        let envar = `${base}_${k.toUpperCase()}`;
        switch(typeof o[k]){
            case 'string':
                if(typeof process.env[envar] !== 'undefined'){
                    console.log(`--- resetting config val ${k} to ${envar} ---`)
                    o[k] = process.env[envar];
                }
                break;
            case 'object':
                resetConfigFieldsToEnvar(envar, o[k]);
                break;
            default: //i.e. undefined
                //do nothing
                break;
        }
    })
};

resetConfigFieldsToEnvar(`E2E_${active_config.toUpperCase()}`, __config);

//Need to keep axios for calls to /debug/flush
axios.defaults.baseURL = `${config.influx_url}`;


//Object.keys(__config).forEach(k => console.log(`__Config.${k} = ${__config[k]}`));

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
    await axios.get('/debug/flush').catch(async error => {
        console.log("DEBUG error " + JSON.stringify(error))
    });
    delete global.__users;
    global.__users = { 'init': undefined };
};

const removeConfInDocker = async () => {
    await exec_prom('docker exec influx2_solo rm /root/.influxdbv2/configs || true');
};

//for compatibility with old test style - until all are updated
const setupUser = async(newUser) => {

    console.warn("WARNING: call to user old style start " + newUser.username);

    if(newUser.username === 'ENV'){
        await resolveUsernameFromEnv(newUser);
    }


    if(newUser.password === 'ENV'){
        await resolvePasswordFromEnv(newUser);
    }

    await resolveUserTokenBeforeCreate(newUser);

    await setupUserRest(newUser);
    await putUser(newUser);
    await __wdriver.sleep(1000);
    await setUserOrgId(newUser);

};

const setUserOrgId = async(user) => {
    //now grab the first org
    let orgsAPI = new OrgsAPI(new InfluxDB({url: __config.influx_url, token: user.token, timeout: 20000}));

    let orgs = await orgsAPI.getOrgs();

    user.orgid = orgs.orgs[0].id;
};

// user { username: 'name', password: 'password', org; 'orgname', bucket: 'bucketname', token: 'optional_token' }
const setupNewUser = async(newUser) => {

    let user = newUser.toUpperCase() === 'DEFAULT' ? __defaultUser : JSON.parse(newUser);

    if(user.username === 'ENV'){
        await resolveUsernameFromEnv(user);
    }


    if(user.password === 'ENV'){
        await resolvePasswordFromEnv(user);
    }

    await resolveUserTokenBeforeCreate(user);

    console.log(`--- preparing to setup user on ${__config.influx_url}`);

    switch(__config.create_method.toUpperCase()){
        case 'REST':
            console.log(`--- Creating new User '${user.username}' over REST ---`);
            await setupUserRest(user)
            break;
        case 'CLI_DOCKER':
            console.log(`--- Creating new User '${user.username}' with CLI to docker container ${__config.docker_name} ---`);
            await setupUserDockerCLI(user);
            break;
        case 'CLI':
            console.log(`--- Creating new user '${user.username}' with CLI using influx CLI client at ${__config.influx_path} ---`);
            await setupUserCLI(user);
            break;
        case "SKIP":
            console.log(`Skipping creating user '${user.username}' on target deployment ` +
                `at ${__config.host}.  User should already exist there` );
            break;
        default:
            throw `Unkown create user method ${__config.create_method}`
    }

    putUser(user);

    await __wdriver.sleep(1000); //give db chance to update before continuing

    await setUserOrgId(user);
};

const resolvePasswordFromEnv = async(user) => {

    if(user.password.toUpperCase() === 'ENV'){
        let envar = `E2E_${__config.config_id.toUpperCase()}_DEFAULT_USER_PASSWORD`;
        user.password = process.env[envar]
    }

};

const resolveUsernameFromEnv = async(user) => {

    if(user.password.toUpperCase() === 'ENV'){
        let envar = `E2E_${__config.config_id.toUpperCase()}_DEFAULT_USER_USERNAME`;
        user.username = process.env[envar]
    }

};


const resolveUserTokenBeforeCreate = async(user) => {
    if(typeof user.token === 'undefined'){
        return;
    }
    if(user.token.toUpperCase() === 'ENV'){

        let envar = `E2E_${__config.config_id.toUpperCase()}_DEFAULT_USER_TOKEN`;
        user.token = process.env[envar]
    }
};

const setupUserRest = async(user) => {

    const setupAPI = new SetupAPI(new InfluxDB(__config.influx_url));

    setupAPI.getSetup().then(async ({allowed}) => {

        let body = {org: user.org, bucket: user.bucket, username: user.username, password: user.password };

        if(user.token){
            body.token = user.token;
        }

        if(allowed){
            await setupAPI.postSetup({
                body: body,
            });
            console.log(`--- Setup user ${user.username} at ${__config.influx_url} success ---`)
        }else{
          console.error(`--- Failed to setup user ${user.username} at ${__config.influx_url} ---`);
        }
    }).catch(async error => {
        console.error(`\n--- Setup user ${user.username} ended in ERROR ---`);
        console.error(error)
    });
};

const setupUserDockerCLI = async(user) => {

    await removeConfInDocker();

    let command = `docker exec ${__config.docker_name} /usr/bin/influx setup` +
        ` --host ${__config.influx_url}` +
        ` -o ${user.org} -b ${user.bucket} -u ${user.username} -p ${user.password}`;
    if(typeof user.token !== 'undefined'){
        command = command + ` -t ${user.token}`;
    }

    command = command + ' -f';

    await exec_prom(command).catch(async error => {
        console.warn(error);
    });
};

const setupUserCLI = async(user) => {

    let configsPath = `${process.env.HOME}/.influxdbv2/configs`;

    if(await fs.existsSync(configsPath)){
        await console.log(`--- removing configs ${configsPath} ---`);
        await fs.unlink(configsPath, async err => {
            if(err) throw err;
            await console.log(`--- ${configsPath} deleted ---`);
        })
    }

    let command = `${__config.influx_path} setup` +
        ` --host ${__config.influx_url}` +
        ` -o ${user.org} -b ${user.bucket} -u ${user.username} -p ${user.password}`;
    if(typeof user.token !== 'undefined'){
        command = command + ` -t ${user.token}`;
    }

    command = command + ' -f';

    exec(command, async (error, stdout, stderr) => {
        if(error){
            await console.error("ERROR: " + error);
        }
       await console.log(stdout);
       if(stderr){
           await console.error(stderr)
       }
    });

};

//reserved for future solutions
const setupCloudUser = async(userName) => {
    if(__config.config_id !== 'cloud'){
        throw `Attempt to setup cloud user with config ${__config.config_id}`
    }

    if(userName.toUpperCase() !== 'DEFAULT'){
        throw `${userName} not handled.  Currently only DEFAULT cloud user in e2e.conf.json is handled.`
    }
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

//TODO - replace or remove in usages
const signInAxios = async (username) => {

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

//TODO - replace or remove in usages
const endSession = async() => {
    delete axios.defaults.headers.common['Cookie'];
};

const writeData = async (userName, //string
    lines = ['testmeas value=300 ' + (nowNano - (3 * intervalNano)),
        'testmeas value=200 ' + (nowNano - (2 * intervalNano)),
        'testmeas value=100 ' + (nowNano - intervalNano)]) => {

    let user = await getUser(userName);

    const writeAPI = await new InfluxDB({url: __config.influx_url, token: user.token})
        .getWriteApi(user.org, user.bucket, 'ns');

    await writeAPI.writeRecords(lines);

    await writeAPI.close().catch(e => {
        console.error(`ERROR: closing write connection: ${e}`);
        throw e;
    })
};

const query = async(userName, //string
    query // string
) => {

    let user = await getUser(userName);

    const queryApi = await new InfluxDB({url: __config.influx_url, token: user.token})
        .getQueryApi(user.org);

    //need to return array of strings as result

    let result = [];

    return await new Promise((resolve,reject) => {
        queryApi.queryRows(query, {
            next(row, tableMeta) {
                const o = tableMeta.toObject(row);
                result.push(o);
            },
            error(error) {
                reject(error)
            },
            complete() {
                resolve(result);
            },
        });
    }).catch(async error => {
        console.error('Caught Error on Query: ' + error);
        throw error;
    });
};

// TODO - verify usages with changes using client API
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

// TODO - add retentionRules or retention policy rp
//{"name":"ASDF","retentionRules":[],"orgID":"727d19908f30184f","organization":"qa"}
const createBucket = async(orgId, // String
    orgName, //String
    bucketName, //String
) => {

    bucketAPI = new BucketsAPI(new InfluxDB({url: __config.influx_url, token: __defaultUser.token, timeout: 20000}));

    await bucketAPI.postBuckets({body: {name: bucketName, orgID: orgId}});
};

//TODO - create cell and view to attach to dashboard
const createDashboard = async(name, orgId) => {
    const dbdsAPI = new DashboardsAPI(new InfluxDB({url: __config.influx_url, token: __defaultUser.token, timeout: 20000}));
    await dbdsAPI.postDashboards({body: {name: name, orgID: orgId}}).catch(async error => {
        console.log('--- Error Creating dashboard ---');
        console.error(error);
        throw error;
    });
};

const getDashboards = async(userName) => {

    let user = getUser(userName);

    const dbdsAPI = new DashboardsAPI(new InfluxDB({url: __config.influx_url, token: user.token, timeout: 20000}));

    return await dbdsAPI.getDashboards();
};

const getAuthorizations = async(userName) => {

    let user = getUser(userName);

    let authsAPI = new AuthorizationsAPI(new InfluxDB({url: __config.influx_url, token: user.token, timeout: 20000}));

    return await authsAPI.getAuthorizations();
};

// http://localhost:9999/api/v2/labels
// {"orgID":"8576cb897e0b4ce9","name":"MyLabel","properties":{"description":"","color":"#7CE490"}}
const createLabel = async(userName,
    labelName,
    labelDescr,
    labelColor ) =>{

    let user = getUser(userName);

    const lblAPI = new LabelsAPI(new InfluxDB({url: __config.influx_url, token: user.token, timeout: 20000}));

    let lblCreateReq = {body: {name: labelName, orgID: user.orgid,
        properties: { description: labelDescr, color: labelColor }}};

    await lblAPI.postLabels(lblCreateReq).catch(async error => {
       console.log('--- Error Creating label ---');
       console.error(error);
       throw error;
    });
};

const createVariable = async(userName, name, type, values, selected = null ) => {

    let user = getUser(userName);

    let varAPI = new VariablesAPI(new InfluxDB({url: __config.influx_url, token: user.token, timeout: 20000}));

    let parseValues = JSON.parse(values);

    let reSel = selected === null ? selected : JSON.parse(selected);

    return await varAPI.postVariables({body: {name: name,
            orgID: user.orgid,
            arguments: {
                type: type,
                values: parseValues
            },
            selected: reSel
       }
    })
};

const getDocTemplates = async(userName) => {

    let user = getUser(userName);

    let docsAPI = new DocumentsAPI(new InfluxDB({url: __config.influx_url, token: user.token, timeout: 20000, }));

    return await docsAPI.getDocumentsTemplates({orgID: user.orgid});
};

const createTemplateFromFile = async(userName, filepath) => {

    let user = getUser(userName);

    let docsAPI = new DocumentsAPI(new InfluxDB({url: __config.influx_url, token: user.token, timeout: 20000}));


    let content = await readFileToBuffer(process.cwd() + '/' + filepath);
    let newTemplate = JSON.parse(content);
    newTemplate.orgID = user.orgid;

    docsAPI.postDocumentsTemplates({ body: newTemplate});
};

const createAlertCheckFromFile = async(userName, filepath) => {

    let user = getUser(userName);

    let content = await readFileToBuffer(process.cwd() + '/' + filepath);

    let newCheck = JSON.parse(content);

    newCheck.orgID = user.orgid;

    let chkAPI = new ChecksAPI(new InfluxDB({url: __config.influx_url, token: user.token, timeout: 20000}));

    chkAPI.createCheck({body: newCheck});
};

const writeLineProtocolData = async (userName, def) => {

    let define = JSON.parse(def);

    let dataPoints = [];
    let nowMillis = new Date().getTime();
    //line protocol i.e. myMeasurement,host=myHost testField="testData" 1556896326
    let intervals = await getIntervalMillis(define.points, define.start);
    let startMillis = nowMillis - intervals.full;

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

    await writeData(userName, dataPoints);
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
    let pointInterval;
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
    return await fs.readFileSync(filepath, 'utf-8');
};

const removeFileIfExists = async function(filepath){
    if(fs.existsSync(filepath)) {
        fs.unlinkSync(filepath);
    }
};

const removeDownloadFilesByRegex = async function(regex){
  let re = new RegExp(regex)
  await fs.readdir(__config.download_dir, (err, files) => {
        for(var i = 0; i < files.length; i++){
            var match = files[i].match(re);
            if(match !== null){
                fs.unlinkSync(__config.download_dir + '/' +match[0]);
            }
        }
    });
};

const fileExists = async function(filePath){
    return fs.existsSync(__config.download_dir + '/' + filePath);
};

const dumpDownloadDir = async function(){
    console.log("DEBUG __config.download_dir: " + __config.download_dir )
    let files = fs.readdirSync(__config.download_dir);
    for(var file of files){
        console.log("   " + file );
    }
    console.log("DEBUG __config.download_dir/..: " + __config.download_dir )
    files = fs.readdirSync(__config.download_dir + '/..');
    for(var file of files){
        console.log("   " + file );
    }


}

const verifyDownloadFileMatchingRegexFilesExist = async function(regex, callback){
    let re = new RegExp(regex);
    let files = fs.readdirSync(__config.download_dir);

    console.log("DEBUG files to be matched: \n" + files);

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
    console.log("DEBUG wait for file to exist: " + __config.download_dir + "/" + filePath);
    while (totalSleep < timeout){
        if(fs.existsSync(__config.download_dir + '/' + filePath)){
            console.log("DEBUG: Matchced File");
            return true;
        }
        await __wdriver.sleep(sleepTime);
        totalSleep += sleepTime;
    }

    console.error(`Timed out ${timeout}ms waiting for file ${__config.download_dir}/${filePath}`);

    throw `Timed out ${timeout}ms waiting for file ${__config.download_dir}/${filePath}`;

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
       await writeData(__defaultUser.username, [
           `test,gen=gen val=${val} ${current * mil2Nano}`
       ]);
       __liveDataGenRunning = true;
       await sleep(def.pulse)
   }

   console.warn(`Live data generation process stopped ${(new Date()).toISOString()}`);
    __liveDataGenRunning = false;
};

const startLiveDataGen = async function(def){
    if(!__liveDataGenRunning) {
        console.log(`Starting live generator with ${JSON.stringify(def)} ${(new Date()).toISOString()}`);
        __killLiveDataGen = false;
        await dataGenProcess(JSON.parse(def));
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
    signInAxios,
    endSession,
    writeData,
    createDashboard,
    createVariable,
    dumpDownloadDir,
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
    getAuthorizations,
    removeConfInDocker,
    removeFileIfExists,
    removeDownloadFilesByRegex,
    setupNewUser,
    startLiveDataGen,
    stopLiveDataGen,
    fileExists,
    verifyDownloadFileMatchingRegexFilesExist,
    waitForFileToExist
};

//flush()
