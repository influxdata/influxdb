import {Then, When} from 'cucumber';
const dataExplorerSteps = require(__srcdir + '/steps/dataExplorer/dataExplorerSteps.js');

let deSteps = new dataExplorerSteps(__wdriver);

Then(/^the Data Explorer page is loaded$/, {timeout: 2 * 5000}, async () => {
    await deSteps.isLoaded();
    await deSteps.verifyIsLoaded();
    //await deSteps.verifyHeaderContains('Data Explorer');
});

When(/^choose bucket named "(.*)"$/, async (bucket) => {
    await deSteps.chooseBucket(bucket);
});

When(/^choose the item "(.*)" in builder card "(.*)"$/, async (measurement,index) => {
    await deSteps.chooseItemFromBuilderCard(measurement, index);
})

When(/^click the submit button$/, async () => {
    await deSteps.clickQuerySubmitButton();
});

Then(/^the time machine graph is shown$/, async() => {
    await deSteps.verifyGraphVisible();
});

When(/^hover over the graph$/, async() => {
    await deSteps.hoverOverGraph();
});

Then(/^the graph data point infobox is visible$/, async () => {
    await deSteps.verifyGraphDataPointInfoBox();
});

When(/^get the current graph$/, async() => {
    await deSteps.getCurrentGraph();
});

When(/^move horizontally to "(.*)" of the graph$/, async (fraction) => {
    await deSteps.moveToHorizontalFractionOfGraph(fraction);
});

When(/^drag horizontally to "(.*)" of the graph$/, async (fraction) => {
    await deSteps.dragToHorizonatalFractionOfGraph(fraction);
});

Then(/^the graph has changed$/, async() => {
    await deSteps.verifyGraphChange();
});


When(/^Click at the point "(.*)" of the graph$/, async (target) => {
    let fracs = JSON.parse(target);
    await deSteps.clickPointWithinGraphByFractions(fracs);
});

When(/^move vertically to "(.*)" of the graph$/, async (fraction) => {
    await deSteps.moveToVerticalFractionOfGraph(fraction);
});

When(/^drag vertically to "(.*)" of the graph$/, async (fraction) => {
    await deSteps.dragToVerticalFractionOfGraph(fraction);
});

When(/^click the refresh button$/, async () => {
    await deSteps.clickQuerySubmitButton();
});

When(/^click the automatic refresh - paused$/, async () => {
    await deSteps.clickRefreshDropdownPaused();
});

When(/^click the automatic refresh - active$/, async () => {
    await deSteps.clickRefreshDropdownActive();
});

When(/^select the automatic refresh "(.*)"$/, async (item) => {
    await deSteps.selectRefreshDropdownItem(item);
});

When(/^click the time machine add query button$/, async () => {
    await deSteps.clickAddQueryButton();
});

When(/^right click on the time machine query tab named "(.*)"$/, async (title) => {
    await deSteps.rightClickQueryTabTitle(title);
});

When(/^click on the time machine query tab named "(.*)"$/, async (title) => {
    await deSteps.clickQueryTabTitle(title);
});

When(/^click on "(.*)" in the query tab menu$/, async (item) => {
    await deSteps.selectQueryTabMenuItem(item);
});

When(/^input a new query tab name as "(.*)"$/, async (name) => {
    await deSteps.setQueryTabName(name);
});

When(/^click the Script Editor button$/, async () => {
    await deSteps.clickScriptEditorButton();
});

When(/^paste into Script Editor text area$/, { timeout: 20000 }, async text => {
    await deSteps.pasteIntoScriptEditor(text);
});

When(/^click the graph view type dropdown$/, async () => {
    await deSteps.clickViewTypeDropdown();
});

When(/^select the graph view type "(.*)"$/, async (type) => {
    await deSteps.selectViewType(type);
});

Then(/^the graph view type is Graph plus Single Stat$/, async () => {
    await deSteps.verifySingleStatTextVisible();
});

When(/^click the view raw data toggle$/, async () => {
    await deSteps.clickViewRawDataToggle();
});

Then(/^the raw data table is visible$/, async () => {
    await deSteps.verifyRawDataTableVisible();
});

When(/^click the graph time range dropdown$/, async () => {
    await deSteps.clickTimeRangeDropdown();
});

When(/^select the graph time range "(.*)"$/, async (item) => {
    await deSteps.selectTimeRangeDropdownItem(item);
});

When(/^search for function "(.*)"$/, async (funct) => {
    await deSteps.searchFunction(funct);
});

When(/^select the function "(.*)"$/, async (funct) => {
    await deSteps.selectFunctionFromList(funct);
});



