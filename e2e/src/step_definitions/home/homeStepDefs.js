import { Then, When } from 'cucumber';
const homeSteps = require(__srcdir + '/steps/home/homeSteps.js');

let hSteps = new homeSteps(__wdriver);

Then(/^the home page is loaded$/, {timeout: 2 * 5000}, async () => {
    await hSteps.isLoaded();
    await hSteps.verifyIsLoaded();
});

When(/^click logout from the home page$/, async() => {
    await hSteps.clickLogout();
});

When(/^I click the panel "(.*)"$/, async title => {
    await hSteps.clickQuickStartPanel(title);
});

Then(/^the dashboards panel contains a link to "(.*)"$/, async dbdName => {
    await hSteps.verifyDbdPanelDashboard(dbdName);
});

When(/^click the dashboard link to "(.*)"$/, async dbdName => {

    await hSteps.clickDbdPanelDashboard(dbdName);

});

Then(/^the dashboards panel contains links:$/, async links => {
    await hSteps.verifyDashboardLinksInPanel(links);
});

When(/^click the dashboards panel link "(.*)"$/, async link => {
    await hSteps.clickDashboardLinkFromPanel(link);
});
