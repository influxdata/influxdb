const fs = require('fs')
var {Before, After, AfterAll, Status} = require('cucumber');
var {logging} = require('selenium-webdriver');
var perfUtils = require(__srcdir + '/utils/performanceUtils.js');

/*
Before(function (scenario, callback) {
    callback();
});

BeforeAll(async function (scenario, callback) {
//    await __wdriver.get('chrome://settings/clearBrowserData').then(async () => {
//        console.log("DEBUG clearing browser cache");
//        await __wdriver.sleep(3000);
//        await __wdriver.switchTo().activeElement();
////    await __wdriver.findElement(By.css("* /deep/ #clearBrowsingDataConfirm")).click();
//        await __wdriver.findElement(By.css("clearBrowsingDataConfirm")).click();
//        await __wdriver.sleep(3000);
//    }).catch(async err => {
//        console.log("DEBUG caught err " + err);
//        throw err;
//    });
    callback();
})*/

async function writeScreenShot(filename) {
    filename = filename.replace(/\s+/g, '_');
    return await __wdriver.takeScreenshot().then(async (image, err) => {

        await fs.writeFile(filename, image, 'base64', (err) => {
            if (err) {
                console.log(err)
            }
        })

        return image
    })
}

async function writeConsoleLog(filename){
    filename = filename.replace(/\s+/g, '_');
    await __wdriver.manage().logs().get(logging.Type.BROWSER).then(async logs => {
        for(let log in logs){
            fs.appendFileSync(filename, `[${(new Date(parseInt(logs[log].timestamp))).toISOString()}]${logs[log].level}:${logs[log].message}\n`);
        }
    })

}

async function writeDriverLog(filename){
    filename = filename.replace(/\s+/g, '_');
    await __wdriver.manage().logs().get(logging.Type.DRIVER).then(async logs => {
        for(let log in logs){
            fs.appendFileSync(filename, `[${(new Date(parseInt(logs[log].timestamp))).toISOString()}]:${logs[log].message}\n`);
        }
    })

}



let scenarioCt = 0;
let currentFeature = '';

Before(async function (scenario){

    __currentFeature = scenario.sourceLocation.uri.substring(scenario.sourceLocation.uri.lastIndexOf("/") + 1).replace('.','-')
    __currentScenario = scenario.pickle.name.trim();
    //safety kill any live data generator
    if(!await scenarioContainsTag(scenario, '@use-live-data') && __liveDataGenRunning){
        console.log("killing live generator");
        __killLiveDataGen = true;
    }
});

async function scenarioContainsTag(scenario, tag){
    let match = scenario.pickle.tags.find( elem => elem.name === tag)
    //console.log("DEBUG match " + match);
    return match !== undefined;
}

After(async function (scenario /*,   callback */) {

    //__wdriver.sleep(1500) //DEBUG - getting shots out of order
// Handled in cucumber.js
  //  if(!fs.existsSync(`./${__config.screenshot_dir}`)){
  //      fs.mkdir(`./${__config.screenshot_dir}`, () => {})
  //  }

    __reportedResetOnce = true;

    let uri = scenario.sourceLocation.uri
    let feature = uri.substring(uri.lastIndexOf("/") + 1).replace('.','-')
    let name = scenario.pickle.name.trim().replace(' ', '_');
    name = name.replace('/', '-');
    name = name.replace('\\', '-');

    if(feature !== currentFeature){
        scenarioCt = 0;
    }

    if(scenarioCt === 0){
        currentFeature = feature;
    }
 //   let now = new Date()
 //   let nowStr = now.getFullYear().toString() +
 //       (now.getMonth() + 1).toString().padStart(2, '0') +
 //       now.getDate().toString().padStart(2, '0') + "-" +
 //       now.getHours().toString().padStart(2, '0') +
 //       now.getMinutes().toString().padStart(2, '0') +
 //       now.getSeconds().toString().padStart(2, '0')
    //let filebase = __config.screenshot_dir + '/' + feature + "-" + nowStr + "-" + name
    let filebase = __screenShotDir + "/" +  feature + "-" + (scenarioCt++).toString().padStart(2, '0') + "-" + name;
    let world = this;

    if(scenario.result.status === Status.FAILED){
            await writeScreenShot(filebase + "-ERR" + ".png").then(async img => {
                await world.attach(img, 'image/png')
            });
         await writeConsoleLog(filebase + '-ERR-console.log').catch(async e => {
             throw('failed to write ' + filebase + '-ERR-console.log\n' + e);
         });
        await writeDriverLog(filebase + '-ERR-driver.log').catch(async e => {
            throw('failed to write ' + filebase + '-ERR-console.log\n' + e);
        })

    }else {
            await writeScreenShot(filebase + "--OK" + ".png").then(async img => {
                await world.attach(img, 'image/png')
            })
    }

    //callback()

});



AfterAll(async function ( ) {

    __reportedResetOnce = false;

    if(__liveDataGenRunning) {
        console.log("killing live generator");
        __killLiveDataGen = true;
    }

    if(perfUtils.performanceLog.length > 0){
        await perfUtils.writePerformanceLog();
        await perfUtils.writePerfomanceReport();
    }
});




