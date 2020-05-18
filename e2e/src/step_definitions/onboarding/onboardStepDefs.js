import { Given, Then, When } from 'cucumber';

const onboardingSteps = require(__srcdir + '/steps/onboarding/onboardingSteps.js');

let driver = __wdriver;
let onbSteps = new onboardingSteps(driver);

Given(/^I open the Influx onboarding page$/, {timeout: 15000}, async () => {

    //await driver.get("http://" + __config.host + ":" + __config.port + "/" )
    await onbSteps.openBase();

    //await onbSteps.delay(3000)
    //return 'pending';

});

Then(/^there is a Welcome message$/, async () => {

    /*
    expect(await driver.findElement(By.css('[data-testid=init-step--head-main]'))
        .getText())
        .to
        .include('Welcome') */
    await onbSteps.verifyHeadContains('Welcome');
    //return 'pending';

});

Then(/^there is a link to corporate$/, async () => {

    /*
    expect(await driver.findElement(By.css('[data-testid=credits] a')).getText())
        .to
        .equal('InfluxData')

    expect(await driver.findElement(By.css('[data-testid=credits] a')).getAttribute('href'))
        .to
        .equal('https://www.influxdata.com/')
    */
    await onbSteps.verifyCreditsLink();

});

When(/^I click on Get Started$/, async () => {

    //await driver.findElement(By.css('[data-testid=onboarding-get-started]')).click()
    //return 'pending';
    await onbSteps.clickStart();
    //await onbSteps.delay(3000)

});

Then(/^the Initial Setup Page is loaded$/, async () => {
    /*
    expect(await driver.findElement(By.css('[data-testid=admin-step--head-main]')).getText())
        .to
        .include('Setup')

    expect(await driver.findElement(By.css('[data-testid=nav-step--welcome]')).getText())
        .to
        .equal('Welcome')

    expect(await driver.findElement(By.css('[data-testid=nav-step--setup]')).getText())
        .to
        .include('Setup') */
    await onbSteps.verifySetupHeaderContains('Setup');
    await onbSteps.verifyNavCrumbText('welcome', 'Welcome');
    await onbSteps.verifyNavCrumbText('setup', 'Setup');
    //return 'pending';

});

Then(/^the continue button is disabled$/, async () => {
    await onbSteps.verifyContinueButtonDisabled();
});

When(/^enter a new user name "(.*?)"$/, async name => {
    await onbSteps.setInputFieldValue('username', (name === 'DEFAULT') ? __defaultUser.username : name);
    //return "pending";
});

When(/^enter a new password "(.*?)"$/, async password => {
    await onbSteps.setInputFieldValue('password', (password === 'DEFAULT') ? __defaultUser.password : password);
    //return "pending";
});

When(/^enter confirm the new password "(.*?)"$/, async password => {
    await onbSteps.setInputFieldValue('password-chk', (password === 'DEFAULT') ? __defaultUser.password : password);
    //return "pending";
});

Then(/^the form error message says "(.*?)"$/, async message => {
    await onbSteps.verifyFormErrorMessage(message);
});

Then(/^the form error message is not present$/, async () => {
    await onbSteps.verifyFormErrorMessageNotPresent();
});

When(/^enter a new organization name "(.*?)"$/, async orgname => {
    await onbSteps.setInputFieldValue('orgname', (orgname === 'DEFAULT') ? __defaultUser.org : orgname);
    //return "pending";
});

When(/^enter a new bucket name "(.*?)"$/, async bucketname => {
    await onbSteps.setInputFieldValue('bucketname', (bucketname === 'DEFAULT') ? __defaultUser.bucket : bucketname);
    //return "pending";
});

When(/^click next from setup page$/, async () => {
    await onbSteps.clickContinueButton();
    //   return "pending";
});

When(/^click next from setup page without page check$/, async() => {
    await onbSteps.clickContinueButton(false);
});

Then(/^verify ready page$/, async () => {
    await onbSteps.verifySubtitle();
    await onbSteps.verifyNavCrumbText('complete', 'Complete');
    await onbSteps.delay(1000);
    //ideally following should be not an exact match but a general value match e.g. lighter than, darker than
    //also firefox returns different rgb function signature, so just test for rgb values
    await onbSteps.verifyNavCrumbTextColor('complete', '246, 246, 248');
    //return "pending";

});

When(/^click quick start button$/, {timeout: 10000}, async () => {
    //await readyPage.clickQickStart()
    await onbSteps.clickQuickStartButton();
    //return 'pending'
});

When(/^click advanced button$/, {timeout: 10000}, async () => {
    await onbSteps.clickAdvancedButton();
});

Then(/^Fail$/, async() => {
    await onbSteps.failTest();
});


