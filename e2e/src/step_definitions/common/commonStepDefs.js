import { AfterAll, Given, Then, When } from 'cucumber';
import {flush} from '../../utils/influxUtils';
const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const signinSteps = require(__srcdir + '/steps/signin/signinSteps.js');
const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const influxUtils = require(__srcdir + '/utils/influxUtils.js');

let bSteps = new baseSteps(__wdriver);
let sSteps = new signinSteps(__wdriver);
let iSteps = new influxSteps(__wdriver);

Given(/^I reset the environment$/, async () => {
    await bSteps.driver.sleep(1000); //since gets called after scenarios, need a short delay to avoid promise resolution issues
    await flush();
});

/*
Before(() => {
})

BeforeAll(() => {
})

After(() => {
    console.log("DEBUG After hook")
})
*/

AfterAll(async() => {
    await bSteps.driver.close();
});

When(/^clear browser storage$/, async () => {
    await bSteps.clearBrowserLocalStorage();
});

Then(/^the success notification says "(.*?)"$/, async message => {
    await bSteps.isNotificationMessage(message);
});

Then(/^the success notification contains "(.*?)"$/, async text => {
    //can be used in template or outline - some cases may needed to be skipped
    if(text.toLowerCase() !== 'skip') {
        await bSteps.containsNotificationText(text);
    }
});

Then(/^the primary notification contains "(.*)"$/, async text => {
    if(text.toLowerCase() !== 'skip') {
        await bSteps.containsPrimaryNotificationText(text);
    }
});

Then(/^the error notification contains "(.*?)"$/, async text => {
    await bSteps.containsErrorNotificationText(text);
});

When(/^close all notifications$/, async() => {
    await bSteps.closeAllNotifications();
});

// newUser if not DEFAULT should follow {username: 'name', password: 'password', org: 'org', bucket: 'bucket'}
Given(/^run setup over REST "(.*?)"$/, async( newUser ) => {

    await influxUtils.flush();

    if(newUser === 'DEFAULT'){
        await influxUtils.setupUser(__defaultUser);
    }else{
        let user = JSON.parse(newUser);
        if(user.password.length < 8 ){
            throw Error(`Password: ${user.password} is shorter than 8 chars`);
        }
        await influxUtils.setupUser(user);
    }

});

When(/^API sign in user "(.*?)"$/, async username => {
    await influxUtils.signIn((username === 'DEFAULT') ? __defaultUser.username : username).then(async () => {
        // await sSteps.driver.sleep(1500)

    }).catch(async err => {
        console.log('ERROR ' +  err);
        throw(err);
    });
});

When(/^API end session$/, async() => {
    await influxUtils.endSession();
});

When(/^UI sign in user "(.*?)"$/, {timeout: 10000},  async username => {
    let user = influxUtils.getUser((username === 'DEFAULT') ? __defaultUser.username : username);
    await sSteps.signin(user);
    //await sSteps.driver.sleep(1500)
});

When(/^write basic test data for org "(.*?)" to bucket "(.*?)"$/, async (org,bucket) => {
    await influxUtils.writeData(org,bucket);
});

When(/^write sine data for org "(.*?)" to bucket "(.*?)"$/, async (org, bucket) =>{

    let nowNano = new Date().getTime() * 1000000;

    let intervalNano = 600 * 1000 * 1000000; //10 min in nanosecs

    let lines = [];

    let recCount = 256;

    let startTime = nowNano - (recCount * intervalNano);

    for(let i = 0; i < recCount; i++){
        lines[i] = 'sinus point=' + Math.sin(i) + ' ' + (startTime + (i * intervalNano));
    }

    console.log('DEBUG lines: ');
    lines.forEach((line) => {
        console.log(line);
    });

    await influxUtils.writeData(org, bucket, lines);

});

When(/^query sine data for org of user "(.*)" from bucket "(.*)"$/, async (user, bucket) => {
    let startTime = '-1d';
    let org = influxUtils.getUser(user).orgid;
    let query = `from(bucket: "${bucket}")
  |> range(start: ${startTime}) 
  |> filter(fn: (r) => r._measurement == "sinus")
  |> filter(fn: (r) => r._field == "point")`;

    let results = await influxUtils.query(org, query);
    console.log('DEBUG results: ' + results);
});


When(/^API create a dashboard named "(.*?)" for user "(.*?)"$/, async (name, username) => {

    let user = await influxUtils.getUser(username);

    let dashb = await influxUtils.createDashboard(name, user.orgid);

    if(user.dashboards === undefined){
        user.dashboards = new Object();
    }

    //Save dashboard for later use
    user.dashboards[name] =  dashb;

});

When(/^API create a bucket named "(.*)" for user "(.*)"$/, async (bucket, username) => {
    let user = await influxUtils.getUser((username === 'DEFAULT') ? __defaultUser.username : username);
    await influxUtils.createBucket(user.orgid, user.org, bucket);
});

When(/^API create a label "(.*)" described as "(.*)" with color "(.*)" for user "(.*)"$/,
    async (labelName, labelDescr, labelColor, user) => {
        let orgID = influxUtils.getUser((user === 'DEFAULT') ? __defaultUser.username : user).orgid;
        await influxUtils.createLabel(orgID, labelName, labelDescr, labelColor);
    });

When(/^open page "(.*?)" for user "(.*?)"$/, async (page, username) => {

    let user = await influxUtils.getUser((username === 'DEFAULT') ? __defaultUser.username : username);
    let ctx = 'orgs/' + user.orgid;
    if(page !== 'HOME'){
        ctx += `/${page}`;
    }

    await bSteps.openContext(ctx);

    await iSteps.isLoaded();
    //await bSteps.driver.sleep(3000)

});

Then(/^the form element error message is "(.*)"$/, async msg => {
    await bSteps.verifyElementErrorMessage(msg);
});

Then(/^the form element error message is not shown$/, async () => {
    await bSteps.verifyNoElementErrorMessage();
});

Then(/^no form input error icon is shown$/, async () => {
    await bSteps.verifyNoFormInputErrorIcon();
});

Then(/^a form input error icon is shown$/, async () => {
    await bSteps.verifyInputErrorIcon();
});

When(/^click the Popup Wizard continue button$/, {timeout: 15000}, async() => {
    await bSteps.clickPopupWizardContinue();
    //await bSteps.driver.sleep(10000);
});

When(/^click the wizard previous button$/, async () => {
    await bSteps.clickPopupWizardPrevious();
});

When(/^click the Popup Wizard done button$/, async () => {
    await bSteps.clickPopupWizardContinue();
});

Then(/^the popup wizard continue button is disabled$/, async() => {
    await bSteps.verifyWizardContinueButtonDisabled();
});

When(/^dismiss the popup$/, async () => {
    await bSteps.dismissPopup();
});

When(/^click popup cancel button$/, async () => {
    await bSteps.clickPopupCancelBtn();
});

When(/^click popup cancel simple button$/, async () => {
    await bSteps.clickPopupCancelBtnSimple();
});


Then(/^popup is not loaded$/, async () => {
    await bSteps.verifyPopupNotPresent();
});

When(/^click popup submit button$/, async () => {
    await bSteps.clickPopupSubmitButton();
});

Then(/^the popup wizard step state text contains "(.*)"$/, async text => {
    await bSteps.verifyPopupWizardStepStateText(text);
});

Then(/^the popup wizard step is in state "(.*)"$/, async state => {
    await bSteps.verifyPopupWizardStepState(state);
});

Then(/^the popup wizard import file header contains "(.*)"$/, async text => {
    await bSteps.verifyPopupFileUploadHeaderText(text);
});

When(/^generate a line protocol testdata file "(.*)" based on:$/, async (filePath, def) => {
    await influxUtils.genLineProtocolFile(filePath, def);
});

When(/^generate a line protocol testdata for user "(.*)" based on:$/, async (user, def) => {
    await influxUtils.writeLineProtocolData((user === 'DEFAULT')? __defaultUser: await influxUtils.getUser(user),
        def);
});

When(/^create the "(.*)" variable "(.*)" with default "(.*)" for user "(.*)" with values:$/,
    async(type, name, defVal, user, values) => {
    type = type === 'csv' ? 'constant' : type.toLowerCase();
    let orgID = influxUtils.getUser((user === 'DEFAULT') ? __defaultUser.username : user).orgid;
    await influxUtils.createVariable(orgID, name, type, values, defVal)
});

//For troubleshooting - up to 5 min
When(/^wait "(.*)" seconds$/, {timeout: 5 * 60 * 1000}, async secs => {
    await bSteps.driver.sleep(parseInt(secs) * 1000);
});

When(/^force page refresh$/, async ()=> {
    await bSteps.driver.navigate().refresh();
});

When(/^press the "(.*)" key$/, async key => {
    await bSteps.pressKeyAndWait(key);
});

When(/^create a new template from the file "(.*)" for user "(.*)"$/, async (filepath, user) => {
    let orgID = influxUtils.getUser((user === 'DEFAULT') ? __defaultUser.username : user).orgid;
    await influxUtils.createTemplateFromFile(filepath, orgID);
});

When(/^create check over API from file "(.*)" for user "(.*)"$/, async (filepath, user) => {
    let orgID = influxUtils.getUser((user === 'DEFAULT') ? __defaultUser.username : user).orgid;
    await influxUtils.createAlertCheckFromFile(filepath, orgID);
});

When(/^remove file "(.*)" if exists$/, async filePath => {
    await influxUtils.removeFileIfExists(filePath);
});

Then(/^the file "(.*)" has been downloaded$/, async filePath => {
    await bSteps.verifyFileExists(filePath);
});

When(/^remove files "(.*)" if exists$/, async regex => {
    await influxUtils.removeFilesByRegex(regex);
});

Then(/^a file matching "(.*)" exists$/, async regex => {
    await bSteps.verifyFileMatchingRegexExists(regex);
});

When(/^verify first CSV file matching "(.*)" as containing$/, async (path, dataDesc) => {
    let datdescr = JSON.parse(dataDesc);
   await bSteps.verifyFirstCSVFileMatching(path, datdescr);
});

When(/^get console log$/, async () => {
   await bSteps.getConsoleLog();
});

When(/^write message "(.*)" to console log$/, async msg => {
    await bSteps.writeMessageToConsoleLog(msg);
});

When(/^send keys "(.*)"$/, async keys => {
   await bSteps.sendKeysToCurrent(keys);
});

When(/^start live data generator$/, async def => {
   bSteps.startLiveDataGenerator(def);
});

When(/^stop live data generator$/, async () => {
    bSteps.stopLiveDataGenerator();
});

When(/^click the sort type dropdown$/, async () => {
   await bSteps.clickSortTypeDropdown();
});

When(/^click sort by item "(.*)"$/, async item => {
   await bSteps.clickSortByListItem(item);
});

Then(/^the add label popover is not present$/, async () => {
    await bSteps.verifyAddLabelsPopopverNotPresent();
});

Then(/^the add label popover is present$/, async () => {
   await bSteps.verifyAddLabelPopoverVisible();
});

Then(/^the add label popover contains the labels$/, async labels => {
   await bSteps.verifyLabelPopoverContainsLabels(labels);
});

When(/^click the label popover item "(.*)"$/, async item => {
   await bSteps.clickLabelPopoverItem(item);
});

Then(/^the add label popover does not contain the labels:$/, { timeout: 10000}, async labels => {
    await bSteps.verifyLabelPopoverDoesNotContainLabels(labels);
});

When(/^set the label popover filter field to "(.*)"$/, async val => {
   await bSteps.setLabelPopoverFilter(val);
});

Then(/^the label popover contains create new "(.*)"$/, async name => {
   await bSteps.verifyLabelPopoverCreateNew(name);
});

Then(/^the add label popover does not contain create new$/, async () => {
   await bSteps.verifyLabelPopupNoCreateNew();
});

When(/^clear the popover label selector filter$/, async () => {
    await bSteps.clearDashboardLabelsFilter();
});


