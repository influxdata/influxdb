import { Then, When } from 'cucumber';
const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const createOrgSteps = require(__srcdir + '/steps/createOrgSteps.js');

let iSteps = new influxSteps(__wdriver);
let cOrgSteps = new createOrgSteps(__wdriver);

Then(/^influx page is loaded$/, async() => {

    await iSteps.verifyIsLoaded();

});

Then(/^the header contains the org name "(.*?)"$/, async (orgname) => {
    await iSteps.verifyHeaderContains((orgname === 'DEFAULT') ? __defaultUser.org : orgname);
});

Then(/^the home page header contains "(.*)"$/, async content => {
    await iSteps.verifyHeaderContains(content);
});

Then(/^the Create Organization form is loaded$/, {timeout: 2 * 5000}, async () => {
    await cOrgSteps.isLoaded();
    await cOrgSteps.verifyIsLoaded();
});

When(/^hover over the "(.*?)" menu item$/, async (item) => {
    await iSteps.hoverNavMenu(item);
});

Then(/^the home submenu items are "(.*?)"$/, async(state) => {
    await iSteps.verifySubMenuItems('home:heading', state);
    await iSteps.verifySubMenuItems('home:neworg', state);
    await iSteps.verifySubMenuItems('home:logout', state);
});

When(/^click nav sub menu "(.*?)"$/, async(item) => {
    await iSteps.clickSubMenuItem(item);
});

When(/^click user nav item "(.*)"$/, async (item) => {
    await iSteps.clickUserMenuItem(item);
});

When(/^click nav menu item "(.*?)"$/, async(item) => {
    await iSteps.clickMenuItem(item);
});


Then(/^the menu item text "(.*?)" is "(.*?)"$/, async (text, state) => {
    await iSteps.verifyVisibilityNavItemByText(text, state.toLowerCase() !== 'hidden');
});

Then(/^the feedback URL should include "(.*?)"$/, async text => {
    await iSteps.verifyFeedbackLinkContains(text);
});

Then(/^the user menu items are "(.*?)"$/, async(state) => {
    await iSteps.verifyUserMenuItems('switchOrg', state);
    await iSteps.verifyUserMenuItems('createOrg', state);
    await iSteps.verifyUserMenuItems('logout', state);
});
