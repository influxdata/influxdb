import { Then, When } from 'cucumber';
const settingsSteps = require(__srcdir + '/steps/settings/settingsSteps.js');
const variablesSteps = require(__srcdir + '/steps/settings/variablesSteps.js');
const membersSteps = require(__srcdir + '/steps/settings/membersSteps.js');
const labelsSteps = require(__srcdir + '/steps/settings/labelsSteps.js');
const tokensSteps = require(__srcdir + '/steps/loadData/tokensSteps.js');
const orgProfileSteps = require(__srcdir + '/steps/settings/orgProfileSteps.js');
const templatesSteps = require(__srcdir + '/steps/settings/templatesSteps.js');


let setSteps = new settingsSteps(__wdriver);
let varTabSteps = new variablesSteps(__wdriver);
let memTabSteps = new membersSteps(__wdriver);
let tlateTabSteps = new templatesSteps(__wdriver);
let labTabSteps = new labelsSteps(__wdriver);
let tknTabSteps = new tokensSteps(__wdriver);
let opTabSteps = new orgProfileSteps(__wdriver);

Then(/^the Settings page is loaded$/, {timeout: 2 * 5000}, async() => {
    await setSteps.isLoaded();
    await setSteps.verifyIsLoaded();
    await setSteps.verifyHeaderContains('Settings');
});

When(/^click the settings tab "(.*?)"$/,  async(name) => {
    await setSteps.clickTab(name);
});

Then(/^the variables Tab is loaded$/, {timeout: 2 * 5000}, async() => {
    await varTabSteps.isLoaded();
});

Then(/^the templates Tab is loaded$/, {timeout: 2 * 5000}, async() => {
    await tlateTabSteps.isLoaded();
});

Then(/^the labels Tab is loaded$/, {timeout: 2 * 5000}, async() => {
    await labTabSteps.isLoaded();
});

Then(/^the tokens Tab is loaded$/, {timeout: 2 * 5000}, async() => {
    await tknTabSteps.isLoaded();
});

Then(/^the members Tab is loaded$/, {timeout: 2 * 5000}, async() => {
    await memTabSteps.isLoaded();
});

Then(/^the org profile Tab is loaded$/, {timeout: 10000}, async() => {
    await opTabSteps.isLoaded();
});
