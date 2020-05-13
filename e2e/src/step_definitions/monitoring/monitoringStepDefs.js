import { Then, When } from 'cucumber';
const monitoringSteps = require(__srcdir + '/steps/monitoring/monitoringSteps.js');

let monSteps = new monitoringSteps(__wdriver);

Then(/^the Alerting page is loaded$/, async() => {
    await monSteps.isLoaded();
    await monSteps.verifyIsLoaded();
});


Then(/^the notification rules create dropdown is disabled$/, async () => {
    await monSteps.verifyNotifyRulesCreateDropdownDiabled();
});

When(/^click alerting tab "(.*)"$/, async tabName => {
   await monSteps.clickAlertingTab(tabName);
});

When(/^click the create check button$/, async () => {
   await monSteps.clickCreateCheckButton();
});

When(/^click the create check dropdown item "(.*)"$/, async item => {
   await monSteps.clickCreateCheckDropdownItem(item);
});

Then(/^the create check dropodown list contains the items$/, async items => {
   await monSteps.verifyCreateCheckDropdownItems(items);
});

Then(/^the create check dropdown list is not visible$/, async () => {
   await monSteps.verifyCreateCheckDropdownNotVisible();
});

When(/^hover the create check question mark$/, async () => {
   await monSteps.hoverCreateCheckQMark();
});

Then(/^the create check tooltip is visible$/, async () => {
   await monSteps.verifyCreateCheckTooltipVisible();
});

Then(/^the create check tooltip is not visible$/, async () => {
   await monSteps.verifyCreateCheckTooltipNotVisible();
});

When(/^hover the alerts page title$/, async () => {
   await monSteps.hoverPageTitle();
});

When(/^hover the create endpoint question mark$/, async () => {
   await monSteps.hoverCreateEndpointQMark();
});

Then(/^the create endpoint tooltip is visible$/, async () => {
   await monSteps.verifyCreateEndpointTooltipVisible();
});

Then(/^the create endpoint tooltip is not visible$/, async () => {
   await monSteps.verifyCreateEndpointTooltipNotVisible();
});

When(/^hover the create rule question mark$/, async () => {
   await monSteps.hoverCreateRuleQMark();
});

Then(/^the create rules tooltip is visible$/, async () => {
   await monSteps.verifyCreateRuleTooltipVisible();
});

Then(/^the create rules tooltip is not visible$/, async () => {
   await monSteps.verifyCreateRuleTooltipNotVisible();
});

When(/^click create endpoint button$/, async () => {
   await monSteps.clickCreateEndpointButton();
});

Then(/^the create endpoint popup is loaded$/, async () => {
   await monSteps.verifyCreateEndpointPopupLoaded();
});

When(/^click the first time create threshold check$/, async() => {
   await monSteps.clickFirstTimeCreateThresholdCheck();
});

Then(/^the first time create threshold check is visible$/, async () => {
   await monSteps.verifyFirstTimeCreateThresholdCheckVisible();
});

When(/^click the first time create deadman check$/, async () => {
   await monSteps.clickFirstTimeCreateDeadmanCheck();
});

Then(/^the first time create deadman check is visible$/, async () => {
   await monSteps.verifyFirstTimeCreateDeadmanCheckVisible();
});

Then(/^there is an alert card named "(.*)"$/, async name => {
   await monSteps.verifyAlertCardName(name);
});

When(/^hover over the name of the check card "(.*)"$/, async name => {
   await monSteps.hoverOverCheckCardName(name);
});

When(/^click the check card name "(.*)"$/, async name => {
   await monSteps.clickCheckCardName(name);
})

When(/^click the name edit button of the check card "(.*)"$/, async name => {
   await monSteps.clickCheckCardNameEditButton(name);
});

When(/^update the active check card name input to "(.*)"$/, async newVal => {
   await monSteps.updateCheckCardActiveNameInput(newVal);
});

When(/^hover over the description of the check card "(.*)"$/, async name => {
   await monSteps.hoverOverCheckCardDescription(name);
});

When(/^click the description edit button of the check card "(.*)"$/, async name => {
  await monSteps.clickCheckCardDescriptionEditButton(name);
});

When(/^update the active check card description input to:$/, async text => {
   await monSteps.updateCheckCardActiveDescription(text);
});

Then(/^the check card "(.*)" contains the description:$/, async (name,text) => {
   await monSteps.verifyCheckCardDescription(name, text);
});

When(/^click empty label for check card "(.*)"$/, async name => {
   await monSteps.clickCheckCardEmptyLabel(name);
});

When(/^click the checks filter input$/, async () => {
   await monSteps.clickChecksFilterInput();
});

When(/^click the add labels button for check card "(.*)"$/, async name => {
   await monSteps.clickCheckCardAddLabels(name);
});

Then(/^the check card "(.*)" contains the label pills:$/, async (name,labels) => {
   await monSteps.verifyCheckCardLabels(name,labels);
});

When(/^remove the label pill "(.*)" from the check card "(.*)"$/, async (label, name) => {
   await monSteps.removeLabelPillFromCheckCard(name,label);
});

Then(/^the check card "(.*)" does not contain the label pills:$/, {timeout: 10000}, async (name,labels) => {
   await monSteps.verifyCheckCardDoesNotHaveLabels(name, labels);
});

When(/^click the check card "(.*)" clone button$/, async name => {
   await monSteps.clickCheckCardCloneButton(name);
});

When(/^click the check card "(.*)" clone confirm button$/, async name => {
   await monSteps.clickCheckCardCloneConfirm(name);
});

Then(/^the check cards column contains$/, async names => {
   await monSteps.verifyCheckCardsVisible(names);
});

Then(/^the check cards column does not contain$/, {timeout: 10000}, async names => {
   await monSteps.verifyCheckCardsNotPresent(names);
});

When(/^enter into the check cards filter field "(.*)"$/, async value => {
   await monSteps.enterValueToCheckCardsFilter(value);
});

When(/^clear the check cards filter field$/, async () => {
   await monSteps.clearCheckCardsFilter();
});

Then(/^the "(.*)" cards column empty state message is "(.*)"$/, async (col, message) => {
   await monSteps.verifyEmptyChecksStateMessage(col, message);
});

When(/^click open history of the check card "(.*)"$/, async name => {
   await monSteps.clickCheckCardOpenHistory(name);
});

When(/^click open history confirm of the check card "(.*)"$/, async name => {
   await monSteps.clickCheckCardOpenHistoryConfirm(name);
});

When(/^click delete of the check card "(.*)"$/, async name => {
   await monSteps.clickCheckCardDelete(name);
});

When(/^click delete confirm of the check card "(.*)"$/, async name => {
   await monSteps.clickCheckCardDeleteConfirm(name);
});

Then(/^there is no alert card named "(.*)"$/, async name => {
   await monSteps.verifyCheckCardsNotPresent(name);
});


