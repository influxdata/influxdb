import { AfterAll, Given, Then, When } from 'cucumber';
const cloudSteps = require(__srcdir + '/steps/cloudSteps.js');
const signinStepsCloud = require(__srcdir + '/steps/signin/signinStepsCloud.js');

let cSteps = new cloudSteps(__wdriver);
let sStepsCloud = new signinStepsCloud(__wdriver);

When(/^open the cloud page in "(.*?)" milliseconds$/, {timeout: 15000}, async (maxDelay) => {
   await cSteps.openCloudPage(maxDelay);
});

When(/^setup default cloud user$/, async () => {
  await cSteps.setupDefaultCloudUser();
});

When(/^I open the cloud login$/, {timeout: 30000}, async () => {
  await cSteps.openCloudLogin();
});

When(/^log in to the cloud$/, async () => {
  await cSteps.logInToCloud();
});

When(/^open the cloud signin page$/, {timeout: 15000}, async () => {
  await sStepsCloud.openContext(sStepsCloud.signinPageCloud.urlCtx);
});

When(/^wait "(.*?)" with delay "(.*?)"$/, async (sleep, delay) => {
  await cSteps.performanceBogusTest(sleep, delay);
});
