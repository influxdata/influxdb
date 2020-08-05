import { AfterAll, Given, Then, When } from 'cucumber';
const cloudSteps = require(__srcdir + '/steps/cloudSteps.js');

let cSteps = new cloudSteps(__wdriver);

When(/^open the cloud page in "(.*?)" milliseconds$/, {timeout: 15000}, async (maxDelay) => {
   await cSteps.openCloudPage(parseInt(maxDelay));
});

/*
When(/^setup default cloud user$/, async () => {
  await cSteps.setupDefaultCloudUser();
});
*/

When(/^I open the cloud login$/, {timeout: 30000}, async () => {
  await cSteps.openCloudLogin();
});

When(/^log in to the cloud$/, async () => {
  await cSteps.logInToCloud();
});

When(/^log in to the cloud in "(.*)" milliseconds$/, {timeout: 15000}, async (maxDelay) => {
  await cSteps.logInToCloudTimed(parseInt(maxDelay));
});

When(/^I logout to account info in "(.*)" milliseconds$/, {timeout: 15000}, async (maxDelay) => {
  await cSteps.logoutToAccountInfoTimed(parseInt(maxDelay));
});

When(/^I logout to login page in "(.*)" milliseconds$/, {timeout: 15000}, async(maxDelay) => {
  await cSteps.logoutToLoginTimed(parseInt(maxDelay));
});

When(/^wait "(.*?)" with delay "(.*?)"$/, async (sleep, delay) => {
  await cSteps.performanceBogusTest(sleep, parseInt(delay));
});

Then(/^the cloud login page is loaded$/, {timeout: 15000}, async () => {
  await cSteps.verifyCloudLoginPageLoaded();
});
