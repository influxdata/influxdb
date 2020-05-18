import { Then, When } from 'cucumber';

const labelsSteps = require(__srcdir + '/steps/settings/labelsSteps.js');

let lblSteps = new labelsSteps(__wdriver);

Then(/^the create Label popup is loaded$/, {timeout: 10000}, async () => {
    await lblSteps.verifyCreateLabelPopupLoaded();
});

When(/^dismiss the Create Label Popup$/, async () => {
    await lblSteps.dismissCreateLabelPopup();
});

When(/^cancel the create label popup$/, async () => {
    await lblSteps.cancelCreateLabelPopup();
});

When(/^I click the empty state Create Label button$/, async () => {
    await lblSteps.clickCreateLabelButtonEmpty();
});

When(/^I click the header Create Label button$/, async () => {
    await lblSteps.clickCreateLabelButtonHeader();
});

Then(/^the color input color matches the preview color$/, async () => {
    await lblSteps.verifyInputColorMatchesPreview();
});

Then(/^the preview label pill contains "(.*)"$/, async text => {
    await lblSteps.verifyCreateLabelPreviewPillText(text);
});

When(/^enter the value "(.*)" into the label popup name input$/, async text => {
    await lblSteps.typeCreateLabelPopupName(text);
});

When(/^enter the value "(.*)" into the label popup description input$/, async descr => {
    await lblSteps.typeCreateLabelPopupDescription(descr);
});

When(/^clear the label popup name input$/, async () => {
    await lblSteps.clearLabelPopupNameInput();
});

When(/^clear the label popup description input$/, async () => {
    await lblSteps.clearLabelPopupDescriptionInput();
});

When(/^clear the label popup color input$/, async () => {
    await lblSteps.clearLabelPopupColorInput();
});

When(/^enter the value "(.*)" into the label popup color input$/, async value => {
    await lblSteps.typeCreateLabelPopupColor(value);
});

When(/^click the color select button I'm feeling lucky$/, async () => {
    await lblSteps.clickLabelPopupLuckyColor();
});

Then(/^the value in the label popup color input is not "(.*)"$/, async value => {
    await lblSteps.verifyLabelPopupColorInputNotValue(value);
});

When(/^click the label popup color swatch "(.*)"$/, async name => {
    await lblSteps.clickLabelPopupColorSwatch(name);
});

Then(/^the value in the label popup color input is "(.*)"$/, async value => {
    await lblSteps.verifyLabelPopupColorInputValue(value);
});

Then(/^the label popup preview text color is "(.*)"$/, async value => {
    await lblSteps.verifyLabelPopupPreviewTextColor(value);
});

When(/^set the color in the label popup to "(.*)"$/, async color => {
    if(color[0] === '#'){ //string value for color input
        await lblSteps.clearLabelPopupColorInput();
        await lblSteps.typeCreateLabelPopupColor(color);
    }else{ //swatch name
        await lblSteps.clickLabelPopupColorSwatch(color);
    }
});

When(/^click the label popup Create Label button$/, async () => {
    await lblSteps.clickLabelPopupCreateButton();
});

When(/^click the label popup Save Changes button$/, async () => {
    await lblSteps.clickLabelPopupCreateButton();
});

Then(/^there is a label card named "(.*)" in the labels list$/, async name => {
    await lblSteps.verifyLabelCardInList(name);
});

Then(/^the label card "(.*)" has a pill colored "(.*)"$/, async (name, color) => {
    await lblSteps.verifyLabelCardPillColor(name, color);
});

Then(/^the label card "(.*)" has description "(.*)"$/, async (name, descr) => {
    await lblSteps.verifyLabelCardDescription(name, descr);
});

When(/^I click the Label Card Pill "(.*)"$/, async name => {
    await lblSteps.clickLabelCardPill(name);
});

Then(/^the edit label popup is loaded$/, async () => {
    await lblSteps.verifyEditLabelPopuLoaded();
});

Then(/^the first labels are sorted as "(.*)"$/, async labels => {
    await lblSteps.verifyLabelSortOrder(labels);
});

When(/^click sort label by name$/, async () => {
    await lblSteps.clickLabelSortByName();
});

When(/^click sort label by description$/, async () => {
    await lblSteps.clickLabelSortByDescription();
});

When(/^clear the labels filter input$/, async () => {
    await lblSteps.clearLabelFilterInput();
});

When(/^enter the value "(.*)" into the label filter$/, async text => {
    await lblSteps.enterTextIntoLabelFilter(text);
});

Then(/^the labels "(.*)" are not present$/, {timeout: 15000}, async labels => {
    await lblSteps.verifyLabelsNotPresent(labels);
});

When(/^hover over label card "(.*)"$/, async name => {
    await lblSteps.hoverOverLabelCard(name);
});

When(/^click delete for the label card "(.*)"$/, async name => {
    await lblSteps.clickLabelCardDelete(name);
});

When(/^click delete confirm for the label card "(.*)"$/, async name => {
    await lblSteps.clickLabelCardDeleteConfirm(name);
});
