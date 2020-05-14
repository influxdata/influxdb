import { Then, When } from 'cucumber';
const checkEditSteps = require(__srcdir + '/steps/monitoring/checkEditSteps.js');

let ckEdSteps = new checkEditSteps(__wdriver);

Then(/^the edit check overlay is loaded$/, async() => {
    await ckEdSteps.isLoaded();
    await ckEdSteps.verifyIsLoaded();
});

Then(/^the edit check overlay is not loaded$/, async () => {
   await ckEdSteps.verifyIsNotLoaded();
});

When(/^click the check editor save button$/, async () => {
   await ckEdSteps.clickCKEdSaveButton();
});

When(/^dismiss edit check overlay$/, async () => {
   await ckEdSteps.dismissOverlay();
});

When(/^enter the alert check name "(.*)"$/, async name => {
    await ckEdSteps.enterAlertCheckName(name);
});

Then(/^the current edit check name is "(.*)"$/, async name => {
    await ckEdSteps.verifyEditCheckName(name);
});

When(/^click check editor configure check button$/, async () => {
   await ckEdSteps.clickCkEdConfigureCheck();
});

When(/^click checkeditor define query button$/, async () => {
   await ckEdSteps.clickDefineQuery();
});

Then(/^the configure check view is loaded$/, async () => {
   await ckEdSteps.verifyConfigureCheckStepLoaded();
});

Then(/^the create check checklist contains:$/, async items => {
    await ckEdSteps.verifyChecklistPopoverItems(items);
});

Then(/^the create check checklist is not present$/, async () => {
   await ckEdSteps.verifyConfigureCheckListPopoverNotPresent();
});

Then(/^the save check button is disabled$/, async () => {
   await ckEdSteps.verifySaveCheckDisabled();
});

Then(/^the save check button is enabled$/, async () => {
   await ckEdSteps.verifySaveCheckEnabled();
});

Then(/^the interval indicator is set to "(.*)"$/, async duration => {
   await ckEdSteps.verifyCkEdIntervalInput(duration);
});

When(/^click on check interval input$/, async () => {
   await ckEdSteps.clickCkEdIntervalInput();
});

When(/^set the check interval input to "(.*)"$/, async duration => {
   await ckEdSteps.setCheckIntervalInput(duration);
});

When(/^enter into interval offset "(.*)"$/, async offset => {
   await ckEdSteps.enterIntoIntervalOffset(offset);
});

Then(/^the check interval hint dropdown list is not visible$/, async () => {
   await ckEdSteps.verifyCkEdHintDropdownNotVisible();
});

Then(/^the check interval hint dropdown list includes$/, async items => {
   await ckEdSteps.verifyCkEdHintDropdownItems(items);
});

When(/^click the interval hint dropdown list item "(.*)"$/, async item => {
    await ckEdSteps.clickCkEdHintDropdownItem(item);
});

When(/^click the check offset interval input$/, async () => {
   await ckEdSteps.clickCkEdOffsetInput();
});

When(/^set the check offset interval input "(.*)"$/, async val => {
   await ckEdSteps.setCheckOffsetInput(val);
});

Then(/^the check offset hint dropdown list is not visible$/, async () => {
    await ckEdSteps.verifyCkEdHintDropdownNotVisible();
});

Then(/^the check offset hint dropdown list includes$/, async items => {
    await ckEdSteps.verifyCkEdHintDropdownItems(items);
});

When(/^click the offset hint dropdown list item "(.*)"$/, async item => {
    await ckEdSteps.clickCkEdHintDropdownItem(item);
});

Then(/^the offset input is set to "(.*)"$/, async val => {
    ckEdSteps.verifyCkEdOffsetInput(val);
});

When(/^update the check message template to$/, async content => {
   await ckEdSteps.updateChecMessageTemplateContent(content);
});

Then(/^the check message tempate contains$/, async content => {
   await ckEdSteps.verifyCheckMessageTemplateContent(content);
});

When(/^click add threshold condition "(.*)"$/, async threshold => {
   await ckEdSteps.clickAddThresholdCondition(threshold);
});

When(/^click the threshold definition dropdown for condition "(.*)"$/, async threshold => {
   await ckEdSteps.clickThresholdDefinitionDropdown(threshold);
});

Then(/^the threshold definition dropdown for "(.*)" contain items:$/, async (threshold, items) => {
   await ckEdSteps.verifyThresholdDefinitionDropdownItems(threshold,items);
});

When(/^click the threshold definition dropodown item "(.*)" for condition "(.*)"$/,
    async (item, threshold) => {
   await ckEdSteps.clickThresholdDefinitionDropdownItem(threshold, item);
});

When(/^set the unary boundary value for the threshold definition "(.*)" to "(.*)"$/,
   async (threshold, val1) => {
    await ckEdSteps.setUnaryThresholdBoundaryValue(threshold, val1);
});

Then(/^there is a binary boundary for the threshold "(.*)" with values "(.*)" and "(.*)"$/,
    async (threshold, lower, upper) => {
    await ckEdSteps.verifyBinaryThresholdBoundaryValues(threshold, lower, upper);
});

Then(/^there is a unary boundary for the threshhold "(.*)" with the value "(.*)"$/, async (threshold, val) => {
   await ckEdSteps.verifyUnaryThresholdBoundaryValue(threshold, val);
});

When(/^set the binary boundary for the threshold "(.*)" from "(.*)" to "(.*)"$/,
    async (threshold, lower, upper) => {
    await ckEdSteps.setBinaryThresholdBoundaryValues(threshold, lower, upper);
});

When(/^click the deadman definition No Values For input$/, async () => {
   await ckEdSteps.clickNoValuesForDurationInput();
});

When(/^set the value of the deadman definition No Values for input to "(.*)"$/, async val => {
   await ckEdSteps.setValueNoValuesForDurationInput(val);
});

Then(/^the deadman definition hints dropdown contains:$/, async items => {
   await ckEdSteps.verifyNoValuesForDurationHintItems(items);
});

When(/^click the deadman definition hint dropdown item "(.*)"$/, async item => {
   await ckEdSteps.clickNoValuesForDurationHintItem(item);
});

Then(/^the deadman definition No Values For input contains "(.*)"$/, async val => {
   await ckEdSteps.verifyValueOfNoValuesForDurationInput(val);
});

When(/^click the deadman definition level dropdown$/, async () => {
   await ckEdSteps.clickDefinitionLevelDropdown();
});

Then(/^the deadman definition level dropdown contains:$/, async items => {
   await ckEdSteps.verifyDefinitionLevelDropdownItems(items);
});

When(/^click the deadman definition level dropdown item "(.*)"$/, async item => {
   await ckEdSteps.clickDefinitionLevelDropdownItem(item);
});

Then(/^the deadman definition level dropdown selected item is "(.*)"$/, async val => {
   await ckEdSteps.verifyDefinitionLevelDropdownSelected(val);
});

When(/^click the deadman definition Stop Checking input$/, async () => {
   await ckEdSteps.clickStopCheckingDurationInput();
});

Then(/^the deadman definition stop hints dropdown contains:$/, async items => {
   await ckEdSteps.verifyDefinitionStopDropdownItems(items);
});

When(/^click the deadman definition stop hint dropdown item "(.*)"$/, async item => {
   await ckEdSteps.clickDefinitionStopDropdownItem(item);
});

Then(/^the deadman definition stop input contains "(.*)"$/, async val => {
   await ckEdSteps.verifyDefinitionStopInputValue(val);
});

When(/^set the value of the definition stop input to "(.*)"$/, async val => {
  await ckEdSteps.setValueDefinitionStopInput(val);
});

Then(/^the time machine cell edit preview contains threshold markers:$/, async markers => {
   await ckEdSteps.verifyCellEditPreviewThresholdMarkers(markers);
});

When(/^click the edit check add tag button$/, async () => {
   await ckEdSteps.clickAddTag();
});

When(/^set the check tag key of tag "(.*)" to "(.*)"$/, async (index, key) => {
   await ckEdSteps.setCheckTagKey(index, key);
});

When(/^set the check tag value of tag "(.*)" to "(.*)"$/, async (index,val) => {
   await ckEdSteps.setCheckTagVal(index, val);
});

When(/^remove check tag key "(.*)"$/, async index => {
   await ckEdSteps.removeCheckTag(index);
});

