import { Then, When } from 'cucumber';
const signinSteps = require(__srcdir + '/steps/signin/signinSteps.js');

let sSteps = new signinSteps(__wdriver);

When(/^open the signin page$/, async () => {
    await sSteps.openContext(sSteps.signinPage.urlCtx);
});

Then(/^the sign in page is loaded$/, async () => {
    await sSteps.verifyIsLoaded();
});

Then(/^the heading contains "(.*?)"$/, async text => {
    await sSteps.verifyHeadingContains(text);
});

Then(/^the InfluxData heading is visible$/, async () => {
   await sSteps.verifyHeading();
});

Then(/^the version shown contains "(.*?)"$/, async version => {
    await sSteps.verifyVersionContains((version == 'DEFAULT') ? __config.influxdb.version : version);
});

Then(/^the credits are valid$/, async () => {
    await sSteps.verifyCreditsLink();
});

When(/^enter the username "(.*?)"$/, async name => {
    await sSteps.enterUsername((name === 'DEFAULT') ? __defaultUser.username : name);
});

When(/^enter the password "(.*?)"$/, async password => {
    await sSteps.enterPassword((password === 'DEFAULT') ? __defaultUser.password : password);
});

When(/^click the signin button$/, async () => {
    await sSteps.clickSigninButton();
    await sSteps.driver.sleep(3000);
});

