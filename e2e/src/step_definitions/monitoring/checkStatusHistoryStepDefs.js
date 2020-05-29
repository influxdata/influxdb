import { Then, When } from 'cucumber';
const checkStatusHistorySteps = require(__srcdir + '/steps/monitoring/checkStatusHistorySteps.js');

let ckStatHistSteps = new checkStatusHistorySteps(__wdriver);

Then(/^the Check statusses page is loaded$/, async() => {
    await ckStatHistSteps.isLoaded();
    await ckStatHistSteps.verifyIsLoaded();
});

Then(/^there are at least "(.*)" events in the history$/, async count => {
   await ckStatHistSteps.verifyMinimumEvents(count);
});

Then(/^event no "(.*)" contains the check name "(.*)"$/, async (index, name) => {
   await ckStatHistSteps.verifyEventName(index, name);
});

When(/^click the check name of event no "(.*)"$/, async index => {
   await ckStatHistSteps.clickEventName(index);
});

Then(/^there is at least "(.*)" events at level "(.*)"$/, async (count, level) => {
   await ckStatHistSteps.verifyMinimumCountEventsAtLevel(count, level);
});

When(/^click event history filter input$/, async () => {
   await ckStatHistSteps.clickEventFilterInput();
});

Then(/^the event history examples dropdown is visible$/, async () => {
   await ckStatHistSteps.verifyFilterExamplesDropdownVisible();
});

Then(/^the event history examples dropdown is not visible$/, async () => {
   await ckStatHistSteps.verifyFilterExamplesDropdownNotVisible();
});

When(/^get events history graph area$/, async () => {
   await ckStatHistSteps.getEventsHistoryGraph();
});

Then(/^the events history graph has changed$/, async () => {
   await ckStatHistSteps.verifyEventsHistoryGraphChanged();
});

When(/^get event marker types and locations$/, async () => {
   await ckStatHistSteps.getEventMarkerTypesAndLocations();
});

When(/^click the alert history title$/, async () => {
   await ckStatHistSteps.clickAlertHistoryTitle();
});

When(/^zoom into event markers$/, async () => {
   await ckStatHistSteps.zoomInOnEventMarkers();
});

Then(/^the event marker locations have changed$/, async () => {
   await ckStatHistSteps.verifyEventMarkerLocationChanges();
});

Then(/^the event toggle "(.*)" is off$/, async event => {
   await ckStatHistSteps.verifyEventToggleOff(event);
});

Then(/^the event toggle "(.*)" is on$/, async event => {
   await ckStatHistSteps.verifyEventToggleOn(event);
});

When(/^double click history graph area$/, async () => {
   await ckStatHistSteps.doubleClickHistoryGraph();
});
