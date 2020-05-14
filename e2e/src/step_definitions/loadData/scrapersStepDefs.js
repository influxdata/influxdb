import { Then, When } from 'cucumber';

const scrapersSteps = require(__srcdir + '/steps/loadData/scrapersSteps.js');
let scrTabSteps = new scrapersSteps(__wdriver);


Then(/^there is a scraper card for "(.*)"$/, {timeout: 10000}, async card => {
    await scrTabSteps.verifyExistsCardByName(card);
});

Then(/^the scraper card named "(.*)" has the bucket "(.*)"$/, async (scraper, bucket) => {
    await scrTabSteps.verifyScraperCardHasBucket(scraper, (bucket === 'DEFAULT') ? __defaultUser.bucket : bucket);
});

Then(/^the scraper card named "(.*)" has the endpoint "(.*)"$/, async (scraper, endpoint) => {
    await scrTabSteps.verifyScraperCardHasEndpoint(scraper, endpoint);
});

Then(/^the scrapers tab is loaded$/, {timeout: 10000}, async () => {
    await scrTabSteps.verifyScrapersTabIsLoaded();
});

When(/^click the create scraper button empty$/, async () => {
    await scrTabSteps.clickCreateScraperButtonEmpty();
});

When(/^click the create scraper button from the header$/, async () => {
    await scrTabSteps.clickCreateScraperButtonInHeader();
});

Then(/^the create scraper button empty is no longer present$/, async() => {
    await scrTabSteps.verifyCreateScraperEmptyNotPresent();
});

Then(/^the scraper name sort order is "(.*)"$/, async(items) => {
    await scrTabSteps.verifyScrapersSortOrderByName(items);
});

When(/^enter the value "(.*)" into the scraper filter$/, async value => {
    await scrTabSteps.enterScrapersFilterValue(value);
});

When(/^clear the scraper filter$/, async () => {
    await scrTabSteps.clearScraperFilter();
});

Then(/^the scraper card "(.*)" is no longer present in the list$/, async scraper => {
    await scrTabSteps.verifyScraperCardNotPresent(scraper);
});

When(/^click the scraper sort by name button$/, async () => {
    await scrTabSteps.clickScraperNameSortButton();
});

When(/^click the scraper sort By URL button$/, async() => {
    await scrTabSteps.clickScraperURLSortButton();
});

When(/^click the scraper sort By Bucket button$/, async() => {
    await scrTabSteps.clickScraperBucketSortButton();
});

When(/^hover over the scraper card name "(.*)"$/, async name => {
    await scrTabSteps.mouseOverScraperCardName(name);
});

When(/^click the scraper card name edit control for the card "(.*)"$/, async name => {
    await scrTabSteps.clickScraperCardNameEditButton(name);
});

When(/^Enter the value "(.*)" for the card "(.*)"$/, {timeout: 10000}, async ( newName, oldName) => {
    await scrTabSteps.enterNewScraperName(newName, oldName);
    //await scrTabSteps.driver.sleep(5000);
});

Then(/^the named query "(.*)" by user "(.*)" on the bucket "(.*)" contains the values "(.*)"$/,
    async (queryName, user, bucket, values) => {
        await scrTabSteps.verifyNamedQueryResponseValues(queryName,
            user,
            (bucket === 'DEFAULT') ? __defaultUser.bucket : bucket,
            values);
    });

Then(/^the delete button of the scraper card named "(.*)" is not present$/, async name => {
    await scrTabSteps.verifyScraperCardDeleteNotPresent(name);
});

When(/^hover over scraper card named "(.*)"$/, async name => {
    await scrTabSteps.hoverOverScraperCard(name);
});

When(/^click the delete button of the scraper card named "(.*)"$/, async name => {
    await scrTabSteps.clickScraperCardDeleteButton(name);
});

When(/^click the confirm delete button of the scraper card named "(.*)"$/, async name => {
    await scrTabSteps.clickScraperCardDeleteConfirm(name);
});


