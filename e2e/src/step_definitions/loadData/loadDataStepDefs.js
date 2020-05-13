import { Then, When } from 'cucumber';
const bucketsSteps = require(__srcdir + '/steps/loadData/bucketsSteps.js');
const telegrafsSteps = require(__srcdir + '/steps/loadData/telegrafsSteps.js');
const scrapersSteps = require(__srcdir + '/steps/loadData/scrapersSteps.js');
const loadDataSteps = require(__srcdir + '/steps/loadData/loadDataSteps.js');

let bktTabSteps = new bucketsSteps(__wdriver);
let teleTabSteps = new telegrafsSteps(__wdriver);
let scrTabSteps = new scrapersSteps(__wdriver);
let ldSteps = new loadDataSteps(__wdriver);

Then(/^the buckets tab is loaded$/, {timeout: 2 * 5000}, async() => {
    await bktTabSteps.isLoaded();
});

Then(/^the Telegraf Tab is loaded$/, {timeout: 2 * 5000}, async() => {
    await teleTabSteps.isLoaded();
});

Then(/^the Scrapers Tab is loaded$/, {timeout: 2 * 5000}, async() => {
    await scrTabSteps.isLoaded();
});

Then(/^the Create Scraper popup is loaded$/, async() => {
    await ldSteps.createScraperPopupLoaded();
});

When(/^dismiss the Create Scraper popup$/, async () => {
    await ldSteps.dismissCreateScraperPopup();
});

Then(/^the Create Scraper popup is no longer present$/, {timeout: 2 * 5000}, async () => {
    await ldSteps.verifyCreateScraperPopupNotPresent();
});

When(/^cancel the Create Scraper popup$/, async () => {
    await ldSteps.cancelCreateScraperPopup();
});

When(/^clear the Scraper Popup name input$/, async () => {
    await ldSteps.clearCreateScraperNameInput();
});

When(/^clear Scraper Popup the Target Url input$/, async () => {
    await ldSteps.clearCreateScraperUrlInput();
});

Then(/^the Create Scrapper popup create button is disabled$/, async () => {
    await ldSteps.verifyCreateScraperSubmitEnabled(false);
});

Then(/^the Create Scrapper popup create button is enabled$/, async () => {
    await ldSteps.verifyCreateScraperSubmitEnabled(true);
});

When(/^enter the name "(.*)" into the Create Scraper popup name input$/, async name => {
    await ldSteps.enterCreateScraperName(name);
});

When(/^enter the value "(.*)" into the Create Scraper popup url input$/, async url => {
    await ldSteps.enterCreateScraperTargetURL(url);
});

When(/^click the Create Scrapper buckets dropdown$/, async () => {
    await ldSteps.clickCreateScraperBucketsDropdown();
});

When(/^select the Scrapper buckets dropdown item "(.*)"$/, async (item) => {
    await ldSteps.selectCreateScraperBucketsItem((item === 'DEFAULT') ? __defaultUser.bucket : item);
});

Then(/^an item for the bucket "(.*)" is an item in the buckets dropdown$/, async item => {
    await ldSteps.verifyCreateScraperBucketsDropdownItem((item === 'DEFAULT') ? __defaultUser.bucket : item);
});

Then(/^NO items in the buckets dropdown are shown$/, async () => {
    await ldSteps.verifyNoBucketItemsInBucketsDropdownShown();
});

When(/^click the create scraper create button$/, async () => {
    await ldSteps.clickCreateScraperBucketCreateButton();
});

When(/^click load data tab "(.*)"$/, async (tab) => {
    await ldSteps.clickTab(tab);
});

Then(/^the Create Telegraf Config Wizard is loaded$/, async () => {
    await ldSteps.verifyCreateTelegrafWizardLoaded();
});

When(/^click the buckets dropdown button$/, async () => {
    await ldSteps.clickBucketsDropdown();
});

When(/^select the buckets dropdown item "(.*)"$/, async item => {
    await ldSteps.selectBucketsDropdownItem((item === 'DEFAULT') ? __defaultUser.bucket : item);
});

When(/^select the telegraf plugin tile "(.*)"$/, async tile => {
    await ldSteps.selectTelegrafWizardPluginTile(tile);
});

When(/^enter the telegraf name "(.*)"$/, async name => {
    await ldSteps.enterTelegrafWizardName(name);
});


