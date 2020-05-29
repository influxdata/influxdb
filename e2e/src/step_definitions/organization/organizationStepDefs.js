import { Then, When } from 'cucumber';

const organizationSteps = require(__srcdir + '/steps/organization/organizationSteps.js');


let orgSteps = new organizationSteps(__wdriver);


Then(/^the Organization page is loaded$/, {timeout: 2 * 5000}, async() => {
    await orgSteps.isLoaded();
    await orgSteps.verifyIsLoaded();
    await orgSteps.verifyHeaderContains('Organization');
});