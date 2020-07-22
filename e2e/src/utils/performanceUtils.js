const assert = require('chai').assert;
const fs = require('fs');

let performanceLog = [];
let performanceRecFile = './report/performance.csv';

//performance record
//let rec = { name: string, state: string, expected: long, start: long, stop: long, duration: long, message: string }

const execTimed = async ( func, maxDelay, failMsg, successMsg) => {

    if(typeof(maxDelay) !== 'number'){
        throw `maxDelay must be of type number.  Got ${typeof(maxDelay)}`
    }

    let start = new Date();
    let startl = start.getTime();
    await func();
    let finish = new Date();
    let finishl = finish.getTime();
    try{
        assert.isBelow(finishl-startl, maxDelay, failMsg);
        performanceLog.push({name: `${__currentFeature}: ${__currentScenario}`,
            state: 'pass',
            maxTime: maxDelay,
            duration: finishl - startl,
            delta: maxDelay - (finishl - startl),
            start: start.toISOString(),
            message: (typeof(successMsg) === 'undefined')? 'success' : successMsg,
        });
    }catch(err){
        performanceLog.push({name: `${__currentFeature}: ${__currentScenario}`,
            state: 'fail',
            maxTime: maxDelay,
            duration: finish - start,
            delta: maxDelay - (finish - start),
            start: start.toISOString(),
            message: (typeof(failMsg) === 'undefined') ? 'failure' : failMsg,
        });
        throw err;
    }
};

const writePerformanceLog = async () => {
    if(performanceLog.length < 1){
        return;
    }
    console.log('\nPerformance');
    let header = [];
    for(let prop in performanceLog[0]){
        if(performanceLog[0].hasOwnProperty(prop)){
            header.push(`${prop}`);
        }
    }

    let fieldSizes = {};
    for(let field of header){
        fieldSizes[field] = field.length;
    }

    //console.log(JSON.stringify(fieldSizes));

    for(let log of performanceLog){
        for(let field of header){
            if(log[field].toString().length > fieldSizes[field]){
                fieldSizes[field] = log[field].toString().length;
            }
        }
    }

    for(let field of header){
        fieldSizes[field] += 2;
    }

    let headerString = '';

    for(let f of header){
        headerString += f.padEnd(fieldSizes[f]) + "| ";
    }

    console.log(headerString);

    //add divider
    console.log('-'.padEnd(Object.values(fieldSizes).reduce((a,b) => a+b+2), '-'));
    let successCt = 0;
    let failCt = 0;
  for(let log of performanceLog){
     let row = '';
     for(let f of header){
         row += log[f].toString().padEnd(fieldSizes[f]) + "| ";
     }

       if(log.state === 'fail'){
          console.log('\x1b[31m%s\x1b[0m', row);
          failCt++;
       }else{
          console.log('\x1b[32m%s\x1b[0m', row);
          successCt++;
       }
  }
    //add divider
    console.log('-'.padEnd(Object.values(fieldSizes).reduce((a,b) => a+b+2), '-'));
  console.log('\x1b[96m%s\x1b[0m', 'Performance scenarios');
  console.log(`total:   ${performanceLog.length}`);
  if(successCt > 0){
      console.log('\x1b[32m%s\x1b[0m', `success: ${successCt}` );
  }
  if(failCt > 0){
      console.log('\x1b[31m%s\x1b[0m', `fail:    ${failCt}` );
  }
  console.log()
};

const writePerfomanceReport = async (filename = performanceRecFile) => {

    let header = [];

    for(let prop in performanceLog[0]){
        if(performanceLog[0].hasOwnProperty(prop)){
            header.push(`${prop}`);
        }
    }

    //write header if report does not yet exist
    if(!fs.existsSync(filename)){

        for(let f of header){
            await fs.appendFileSync(filename, `${f},`)
        }

        await fs.appendFileSync(filename,'\n');

    }


    for(let log of performanceLog) {
        for(let f of header){
            await fs.appendFileSync(filename, `${log[f].toString()},`);
        }
        await fs.appendFileSync(filename, '\n');
    }
};

module.exports = {
    execTimed,
    performanceLog,
    writePerformanceLog,
    writePerfomanceReport
};
