import { Then, When } from 'cucumber';
const cellOverlaySteps = require(__srcdir + '/steps/dashboards/cellOverlaySteps.js');

let celOvSteps = new cellOverlaySteps(__wdriver);

Then(/^the cell edit overlay is loaded as "(.*)"$/, {timeout: 10000}, async name => {
    await celOvSteps.verifyCellOverlayIsLoaded(name);
});

When(/^get the current cell edit preview graph$/, async () => {
    await celOvSteps.getCurrentCellEditPreviewGraph();
});

When(/^name dashboard cell "(.*)"$/, async name => {
    await celOvSteps.nameDashboardCell(name);
});

When(/^click dashboard cell edit cancel button$/, async () => {
    await celOvSteps.clickDashboardCellEditCancel();
});

When(/^click dashboard cell save button$/, async () => {
    await celOvSteps.clickDashboardCellSave();
});

When(/^click the cell edit Time Range Dropdown$/, async () => {
    await celOvSteps.clickCellEditTimeRangeDropdown();
});

When(/^select the cell edit Time Range "(.*)"$/, async item => {
    await celOvSteps.selectCellEditTimeRangeItem(item);
});

When(/^click the cell edit Script Editor button$/, async () => {
    await celOvSteps.clickCellEditScriptEditorButton();
});

When(/^paste into cell edit Script Editor$/, { timeout: 20000 }, async text => {
    await celOvSteps.pasteIntoCellEditScriptEditor(text);
});

When(/^clear the cell edit Script Editor$/, { timeout: 20000 }, async () => {
    await celOvSteps.clearCellEditScriptEditor();
});

Then(/^the time machine cell edit submit button is disabled$/, async () => {
    await celOvSteps.verifyCellEditSubmitDisabled();
});

Then(/^the time machine cell edit submit button is enabled$/, async () => {
    await celOvSteps.verifyCellEditSubmitEnabled();
});

When(/^click the time machine cell edit submit button$/, async () => {
    await celOvSteps.clickCellEditSubmitButton();
});

Then(/^the time machine cell edit preview graph is shown$/, async() => {
    await celOvSteps.verifyCellEditPreviewGraphVisible();
});

Then(/^the time machine cell edit preview axes are shown$/, async() => {
    await celOvSteps.verifyCellEditPreviewAxesVisible();
});

Then(/^the cell edit preview graph is changed$/, async() => {
    await celOvSteps.verifyCellEditPreviewGraphChanged();
});

When(/^click the cell edit save button$/, async () => {
    await celOvSteps.clickCellEditSaveButton();
});

When(/^click on the cell edit name$/, async () => {
    await celOvSteps.clickCellEditName();
});

When(/^change the cell edit name to "(.*)"$/, async name => {
    await celOvSteps.updateCellName(name);
});

When(/^click the dashboard cell view type dropdown$/, async () => {
    await celOvSteps.clickViewTypeDropdown();
});

Then(/^the dashboard cell view type dropdown list contains:$/, async itemList => {
    await celOvSteps.verifyViewTypeListContents(itemList);
});

Then(/^the cell view type dropdown list is not present$/, async () => {
    await celOvSteps.verifyViewTypeListNotPresent();
});

When(/^click cell view customize button$/, async () => {
    await celOvSteps.clickCellViewCustomize();
});

Then(/^the view options container is present$/, async () => {
    await celOvSteps.verifyViewOptionsContainerVisible();
});

Then(/^the view options container is not present$/, async () => {
    await celOvSteps.verifyViewOptionsContainerNotPresent();
});

Then(/^the cell view customize button is highlighted$/, async () => {
    await celOvSteps.verifyCellCustomizeButtonHighlight();
});

Then(/^the cell view customize button is not highlighted$/, async () => {
    await celOvSteps.verifyCustomizeButtonNoHighlightd();
});

Then(/^the time machine view empty queries graph is visible$/, async () => {
    await celOvSteps.verifyTMViewEmptyQueriesGraphVisible();
});

Then(/^the time machine view no results is visible$/, async () => {
    await celOvSteps.verifyTMViewNoResultsVisible();
});

When(/^click time machine autorefresh dropdown$/, async () => {
    await celOvSteps.clickTMAutorefreshDropdown();
});

Then(/^the time machine autorefresh dropdown list contains:$/, async itemList => {
    await celOvSteps.verifyAutorefreshListContents(itemList);
});

When(/^select the time machine autorefresh rate "(.*)"$/, async item => {
    await celOvSteps.clickTMAutorefreshItem(item);
});

Then(/^the time machine force refresh button is not present$/, async () => {
    await celOvSteps.verifyTMAutorefreshForceButtonNotPresent();
});

Then(/^the time machine force refresh button is present$/, async () => {
    await celOvSteps.verifyTMAutorefreshForceButtonVisible();
});

Then(/^the time machine autorefresh dropdown list is set to "(.*)"$/, async selected => {
   await celOvSteps.verifyTMAutorefreshDropdownSelected(selected);
});

When(/^click time machine force refresh$/, async () => {
   await celOvSteps.clickTMForceRefresh();
});

Then(/^the time machine Time Range dropdown list contains:$/, async itemList => {
    await celOvSteps.verifyTMTimeRangeDropdownList(itemList);
});

Then(/^the time machine Time Range dropdown list is not present$/, async () => {
    await celOvSteps.verifyTMTimeRangeDropdownListNotPresent();
});

Then(/^the time machine query builder is visible$/, async () => {
    await celOvSteps.verifyTMQueryBuilderVisible();
});

Then(/^the time machine switch to Query Builder warning is present$/, async () => {
    await celOvSteps.verifyTMQueryBuilderSwitchWarnVisible();
});

Then(/^the time machine switch to Query Builder warning is not present$/, async () => {
    await celOvSteps.verifyTMQueryBuilderSwitchWarnNotPresent();
});

Then(/^the time machine flux editor is visible$/, async () => {
    await celOvSteps.verifyTMFluxEditorVisible();
});

When(/^click the cell edit Query Builder button$/, async () => {
    await celOvSteps.clickTMSwitch2QBuilder();
});

When(/^click the cell edit Query Builder confirm button$/, async () => {
    await celOvSteps.clickTMSwitch2QBuilderConfirm();
});

When(/^click the time machine flux editor$/, async () => {
    await celOvSteps.clickTMFluxEditor();
});

When(/^click the filter functions input$/, async () => {
   await celOvSteps.clickTMFilterFunctionsInput();
});

Then(/^the time machine flux editor is not present$/, async () => {
    await celOvSteps.verifyTMFluxEditorNotPresent();
});

Then(/^the edit cell bucket selector contains buckets:$/, async bucketList => {
    await celOvSteps.verifyTMBucketListContents(bucketList);
});

When(/^click the time machine bucket selector item "(.*)"$/, async item => {
    await celOvSteps.driver.sleep(500); //troubleshoot issue with click on wrong item
    await celOvSteps.clickTMBucketSelectorItem(item);
});

Then(/^the bucket "(.*)" is not present in the time machine bucket selector$/, async bucket => {
    await celOvSteps.verifyBucketNotInTMBucketList(bucket);
});

When(/^filter the time machine bucket selector with "(.*)"$/, async value => {
    await celOvSteps.filterBucketListContents(value);
});

When(/^clear the time machine bucket selector filter$/, async () => {
    await celOvSteps.clearBucketSelectorFilter();
});

Then(/^there are "(.*)" time machine builder cards$/, async count => {
    await celOvSteps.verifyTMBuilderCardsSize(count);
});

Then(/^time machine builder card "(.*)" contains:$/, async (index,items) => {
    await celOvSteps.verifyItemsInBuilderCard(index,items);
});

Then(/^the item "(.*)" in builder card "(.*)" is selected$/, async (item,index) => {
   await celOvSteps.verifyItemSelectedInBuilderCard(index, item);
})

Then(/^time machine bulider card "(.*)" contains the empty tag message$/, async index => {
    await celOvSteps.verifyEmptyTagsInBuilderCard(index);
});

Then(/^there are no selected tags in time machine builder card "(.*)"$/, async index => {
    await celOvSteps.verifyNoSelectedTagsInBuilderCard(index);
});

Then(/^the selector count for builder card "(.*)" contains the value "(.*)"$/, async (index,value) =>{
    await celOvSteps.verifySelectorCountInBuilderCard(index,value);
});

Then(/^time machine builder card "(.*)" does not contain "(.*)"$/, async (index, item) => {
    await celOvSteps.verifyItemNotInBuilderCard(index,item);
});

When(/^click the tag selector dropdown of builder card "(.*)"$/, async index => {
    await celOvSteps.clickTagSelectorOfBuilderCard(index);
});

Then(/^the tag selector dropdown of builder card "(.*)" contains:$/, async (index,items) => {
    await celOvSteps.verifyItemsInBuilderCardTagSelector(index,items);
});

When(/^click the tag selector dropdown item "(.*)" of builder card "(.*)"$/, async (item, index) => {
    await celOvSteps.clickTagSelectorDropdownItemInBuilderCard(item,index);
});

When(/^click the tag "(.*)" in builder card "(.*)"$/, async (tag, cardIndex) => {
    await celOvSteps.clickTagInBuilderCard(tag, cardIndex);
});

When(/^filter the tags in time machine builder card "(.*)" with "(.*)"$/, {timeout: 10000}, async (index,term) => {
    await celOvSteps.filterBuilderCardListContents(index,term);
});

Then(/^time machine builder card "(.*)" is empty$/, async index => {
    await celOvSteps.verifyBuilderCardEmpty(index);
});

When(/^clear the tags filter in time machine builder card "(.*)"$/, async index => {
    await celOvSteps.clearTagsFilterInBuilderCard(index);
});

Then(/^the contents of tag selector dropodwn of build card "(.*)" are not present$/, async index => {
    await celOvSteps.verifyBuilderCardTagSelectNotPresent(index);
});

Then(/^the selector counf for builder card "(.*)" is not present$/, async index => {
    await celOvSteps.verifyBuilderCardSelectCountNotPresent(index);
});

Then(/^the delete button for builder card "(.*)" is not present$/, async index => {
    await celOvSteps.verifyBuilderCardDeleteNotPresent(index);
});

When(/^click delete for builder card "(.*)"$/, async index => {
    await celOvSteps.clickBuilderCardDelete(index);
});

Then(/^the time machine query builder function duration period is "(.*)"$/, async duration => {
    await celOvSteps.verifyTMQueryBuilderFunctionDuration(duration);
});

When(/^click the custom function group$/, async () => {
   await celOvSteps.clickCustomFunctionGroup();
});

When(/^click the time machine query builder function duration input$/, async () => {
    await celOvSteps.clickTMQueryBuilderFunctionDuration();
});

When(/^click the time machine query builder function custom duration tab$/, async () => {
   await celOvSteps.clickTMQueryBuilderCustomDuration();
});

When(/^click the time machine query builder function auto duration tab$/, async () => {
   await celOvSteps.clickTMQueryBuilderAutoDuration();
});

Then(/^the query builder function duration suggestion drop down contains "(.*)" suggestions$/, async count => {
    await celOvSteps.verifyTMQBFunctionDurationSuggestionCount(count);
});

Then(/^the query builder function duration suggestion drop down includes$/, async items => {
    await celOvSteps.verifyTMQBFunctionDurationSuggestionItems(items);
});

When(/^click the query builder function duration suggestion "(.*)"$/, async item => {
    await celOvSteps.clickTMQBFunctionDurationSuggestionItem(item);
});

Then(/^the query builder function list contains$/, async items => {
    await celOvSteps.verifyTMQueryBuilderFunctionListItems(items);
});

Then(/^the query builder function list has "(.*)" items$/, async count => {
    await celOvSteps.verifyQuerBuilderFunctionListItemCount(count);
});

When(/^filter the query builder function list with "(.*)"$/, async term => {
    await celOvSteps.filterQueryBuilderFunctionList(term);
});

When(/^clear the query builder function lis filter$/, async () => {
    await celOvSteps.clearQueryBuilderFunctionListFilter();
});

When(/^get metrics of time machine cell edit preview$/, async () => {
    await celOvSteps.getTMPreviewMetrics();
});

When(/^get metrics of time machine query builder$/, async () => {
    await celOvSteps.getTMPQueryAreaMetrics();
});

When(/^get time machine preview canvas$/, async () => {
    await celOvSteps.getTMPreviewCanvas();
});

When(/^get time machine preview axes$/, async () => {
    await celOvSteps.getTMPreviewCanvasAxes();
});

When(/^resize time machine preview area by "(.*)"$/, async dims => {
    let deltaSize = JSON.parse(dims);
    await celOvSteps.resizeTMPreviewBy(deltaSize);
});

Then(/^the time machine preview area has changed by "(.*)"$/, async dims => {
    let deltaSize = JSON.parse(dims);
    await celOvSteps.verifyTMPreviewAreaSizeChange(deltaSize);
});

Then(/^the time machine query builder area has changed by "(.*)"$/, async dims => {
    let deltaSize = JSON.parse(dims);
    await celOvSteps.verifyTMQBAreaSizeChange(deltaSize);
});

Then(/^the time machine preview canvas has changed$/, {timeout: 10000}, async () => {
    await celOvSteps.verifyTMPreviewCanvasChange();
});

Then(/^the time machine preview canvas has not changed$/, {timeout: 1000}, async () => {
   await celOvSteps.verifyTMPreviewCanvasNoChange();
});

Then(/^the time machine preview axes have changed$/, async () => {
    await celOvSteps.verifyTMPreviewAxesChange();
});

Then(/^the time machine preview canvas is not present$/, async () => {
    await celOvSteps.verifyTMPreviewCanvasNotPresent();
});

Then(/^the time machine preview canvas axes are not present$/, async () => {
    await celOvSteps.verifyTMPreviewCanvasAxesNotPresent();
});

When(/^click the time machine query builder add query button$/, async () => {
    await celOvSteps.clickTMAddQuery();
});

Then(/^the bucket selected in the current time machine query is "(.*)"$/, async bucket => {
    await celOvSteps.verifyTMQueryBucketSelected(bucket);
});

Then(/^the tag selected in the current time machine query card "(.*)" is "(.*)"$/, async (index, tag) => {
    await celOvSteps.verifyTMQueryCardSelected(index,tag);
});

Then(/^the functions selected in the current time machine query card are "(.*)"$/, async funcs => {
    await celOvSteps.verifyTMQueryFunctionsSelected(funcs);
});

When(/^click the query builder function "(.*)"$/, async func => {
    await celOvSteps.clickTMQBFunction(func);
});

Then(/^query "(.*)" is the active query in query builder$/, async title => {
    await celOvSteps.verifyTMQBActiveQuery(title);
});

When(/^click on query "(.*)" in the query builder$/, async title => {
    await celOvSteps.clickOnTMQBQueryTab(title);
});

When(/^right click on the time machine query tab title "(.*)"$/, async title => {
    await celOvSteps.rightClickTMQBQueryTabTitle(title);
});

When(/^click the time machine query tab right click menu item "(.*)"$/, async item => {
    await celOvSteps.clickTMQBQueryTabRightClickMenuItem(item);
});

When(/^enter "(.*)" into the time machine query tab name input$/, async name => {
    await celOvSteps.enterNewTMQBQueryTabName(name);
});

Then(/^there is no time machine query tab named "(.*)"$/, async name => {
    await celOvSteps.verifyNoTMQBQueryTabNamed(name);
});

When(/^click hide query of time machine query tab "(.*)"$/, async name => {
    await celOvSteps.clickTMQBHideQuery(name);
});

When(/^right click the time machine query tab "(.*)"$/, async name => {
    await celOvSteps.clickRightTMQBQuery(name);
});

When(/^click delete of time machine query tab "(.*)"$/, async name => {
    await celOvSteps.clickTMQBDeleteQuery(name);
});

Then(/^there are "(.*)" time machine query tabs$/, async count => {
    await celOvSteps.verifyTMQBNumberOfQueryTabs(count);
});

Then(/^the time machine script editor contains$/, async script => {
    await celOvSteps.verifyTMQBScriptEditorContents(script);
});

When(/^change the time machine script editor contents to:$/, { timeout: 60000 }, async script => {
    await celOvSteps.updateTMQBScriptEditorContents(script);
});

When(/^set the time machine script editor contents to:$/, {timeout: 60000}, async script => {
    await celOvSteps.updateTMQBScriptEditorContents(script);
});

When(/^click the time machine switch to query builder button$/, async () => {
    await celOvSteps.clickTMSwitch2QBuilder();
});

Then(/^the time machine empty graph error message is:$/, async msg => {
    await celOvSteps.verifyTMEmptyGraphErrorMessage(msg);
});

When(/^close all time machine builder cards$/, async () => {
    await celOvSteps.closeAllTMQBCards();
});

When(/^unselect any tags in time machine builder card "(.*)"$/, async index => {
    await celOvSteps.deselectAllActiveTagsInTMQBCard(index);
});

Then(/^the time machine query edit function categories are displayed:$/, async cats => {
    await celOvSteps.verifyTMQEFunctionCategoriesDisplayed(cats);
});

When(/^filter the time machine query edit function list with "(.*)"$/, async term => {
    await celOvSteps.filterTMQEFunctionsList(term);
});

When(/^clear the time machine query edit function list filter$/, async () => {
    await celOvSteps.clearTMQEFunctionsListFilter();
});

Then(/^the following function are visible in the time machine function list:$/, async funcs => {
    await celOvSteps.verifyTMQEVisibleFunctions(funcs);
});

Then(/^the following function are not visible in the time machine function list:$/, {timeout: 20000}, async funcs => {
    await celOvSteps.verifyTMQENotVisibleFunctions(funcs);
});

When(/^click the time machine query editor function "(.*)"$/, async func => {
    await celOvSteps.clickTMQEFunction(func);
});

When(/^click inject the time machine query editor function "(.*)"$/, async func => {
    await celOvSteps.clickInjectTMQEFunction(func);
});

When(/^hover over time machine query edit function "(.*)"$/, async func => {
    await celOvSteps.hoverOverTMQEFunction(func);
});

Then(/^the time machine query edit function popup description contains:$/, async text => {
    await celOvSteps.verifyTMQEFunctionPopupDescription(text);
});

Then(/^the time machine query edit function popup snippet contains:$/, async text => {
    await celOvSteps.verifyTMQEFunctionPopupSnippet(text);
});

Then(/^the time machine query edit function popup is not visible$/, async () => {
    await celOvSteps.verifyTMQEFunctionPopupNotVisible();
});

When(/^hover over the time machine query editor timerange dropdown button$/, async () => {
    await celOvSteps.hoverOverTMQETimerangeDropdown();
});

When(/^hover over the time machine query editor submit button$/, async() => {
    await celOvSteps.hoverOverTMCellEditSubmit();
});

When(/^send keys "(.*)" to the time machine flux editor$/, async keys => {
    await celOvSteps.sendKeysToTimeMachineFluxEditor(keys);
});

Then(/^the time machine raw data table is not present$/, async () => {
   await celOvSteps.verifyTMRawDataTableNotPresent();
});

Then(/^the time machine raw data table is present$/, async () => {
   await celOvSteps.verifyTMRawDataTablePresent();
});

When(/^click time machine raw data toggle$/, async () => {
   await celOvSteps.clickTMRawDataToggle();
});

Then(/^time machine raw data cell "(.*)" contains "(.*)"$/, async (coords, value) => {
    let cartesCoords = JSON.parse(coords);
   await celOvSteps.verifyTMRawDataCellContents(cartesCoords, value);
});

When(/^scroll time machine raw data "(.*)"$/, async (d_coords) => {
   let cartesCoords = JSON.parse(d_coords);
   await celOvSteps.scrollTMRawDataTable(cartesCoords);
});

When(/^click time machine download CSV$/, async () => {
   await celOvSteps.clickTMDownloadCSV();
});

When(/^click the time machine script editor variables tab$/, async () => {
   await celOvSteps.clickTMQEVariablesTab();
});

Then(/^the time machine variables list contains$/, async varList => {
   await celOvSteps.verifyTMQEVariablesList(varList);
});

Then(/^the time machine variables list does not contain$/, {timeout: 10000}, async varList => {
   await celOvSteps.verifyTMQWVarieblesListAbsent(varList);
});

When(/^enter the value "(.*)" in the time machine variables filter$/, async value => {
   await celOvSteps.enterTMQEVariableFilterValue(value);
});

When(/^clear the time machine variables filter$/, async () => {
   await celOvSteps.clearTMQEVariablesFilter();
});

When(/^hover over the time machine variable "(.*)"$/, async varname => {
   await celOvSteps.hoverOverTMQEVariable(varname);
});

When(/^click the time machine variable "(.*)"$/, async varname => {
   await celOvSteps.clickTMQEVariable(varname);
});

When(/^click inject the time machine variable "(.*)"$/, async varname => {
   await celOvSteps.clickInjectTMQEVariable(varname);
});

Then(/^the time machine variable popover is not visible$/, async () => {
   await celOvSteps.verifyTMQEVariablePopoverNotVisible();
});

Then(/^the time machine variable popover is visible$/, async () => {
   await celOvSteps.verifyTMQEVariablePopoverVisible();
});

When(/^click time machine popover variable dropodown$/, async () => {
   await celOvSteps.clickTMQEVariablePopoverVarDropdown();
});
