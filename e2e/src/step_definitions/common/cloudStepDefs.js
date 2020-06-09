import { AfterAll, Given, Then, When } from 'cucumber';
const cloudSteps = require(__srcdir + '/steps/cloudSteps.js');

let cSteps = new cloudSteps(__wdriver);

When(/^setup default cloud user$/, async () => {
  await cSteps.setupDefaultCloudUser();
});

When(/^I open the cloud login$/, {timeout: 30000}, async () => {
  await cSteps.openCloudLogin();
});

When(/^log in to the cloud$/, async () => {
  await cSteps.logInToCloud();
});
