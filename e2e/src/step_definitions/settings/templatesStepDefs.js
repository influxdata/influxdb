import { Then, When } from 'cucumber';

const templatesSteps = require(__srcdir + '/steps/settings/templatesSteps.js');

let tpltSteps = new templatesSteps(__wdriver);

Then(/^the templates are sorted as:$/, async templates => {
    await tpltSteps.verifyTemplateCardsSort(templates);
});

When(/^click user templates$/, async () => {
    await tpltSteps.clickUserTemplatesButton();
});

When(/^click empty state import template button$/, async () => {
    await tpltSteps.clickImportTemplateEmptyButton();
});

Then(/^the import template popup is loaded$/, async () => {
    await tpltSteps.verifyImportTemplatePopupLoaded();
});

Then(/^the Import JSON as template button is disabled$/, async () => {
    await tpltSteps.verifyImportTemplatePopupSubmitEnabled(false);
});

Then(/^the Import JSON as template button is enabled$/, async () => {
    await tpltSteps.verifyImportTemplatePopupSubmitEnabled(true);
});

When(/^click header import template button$/, async () => {
    await tpltSteps.clickImportTemplateHeaderButton();
});

Then(/^click the import template paste button$/, async () => {
    await tpltSteps.clickImportTemplatePasteButton();
});

Then(/^the Import Template file upload area is present$/, async () => {
    await tpltSteps.verifyImportTemplateFileUpload(true);
});

Then(/^the Import Template file upload area is not present$/, async () => {
    await tpltSteps.verifyImportTemplateFileUpload(false);
});

Then(/^the Import Template paste JSON text area is present$/, async () => {
    await tpltSteps.verifyImportTemplatePasteJSON(true);
});

Then(/^the Import Template paste JSON text area is not present$/, async () => {
    await tpltSteps.verifyImportTemplatePasteJSON(false);
});

When(/^enter into the Impprt Template paste JSON text area:$/, async text => {
    await tpltSteps.enterTextImportTemplateJSON(text);
});

When(/^click the import template upload button$/, async () => {
    await tpltSteps.clickImportTemplateUploadButton();
});

When(/^upload the template file "(.*)"$/, async filePath => {
    await tpltSteps.uploadTemplateFile(filePath);
});

When(/^click import template popup submit button$/, async () => {
   await tpltSteps.clickImportTemplateSubmitButton();
});

Then(/^there is a template card named "(.*)"$/, async name => {
    await tpltSteps.verifyTemplateCardVisibility(name);
});

When(/^paste contents of "(.*)" to template textarea$/, {timeout: 60000 }, async filepath => {
    await tpltSteps.copyFileContentsToTemplateTextare(filepath);
});

Then(/^a REST template document for user "(.*)" titled "(.*)" exists$/, async (user,title) => {
    await tpltSteps.verifyRESTTemplateDocumentExists(user ,title);
});

When(/^enter the value "(.*)" into the templates filter field$/, async value => {
    await tpltSteps.enterTemplatesFilterValue(value);
});

Then(/^the template cards "(.*)" are not present$/, {timeout: 30000}, async templates => {
    let tempArr = templates.split(',');
    for(let i = 0; i < tempArr.length; i++){
        await tpltSteps.verifyTemplateNotPresent(tempArr[i]);
    }
});

When(/^clear the templates filter$/, async () => {
    await tpltSteps.clearTemplatesFilter();
});

When(/^click templates sort by Name$/, async () => {
    await tpltSteps.clickSortTemplatesByName();
});

When(/^hover over template card named "(.*)"$/, async name => {
    await tpltSteps.hoverOverTemplateCard(name);
});

When(/^click the context delete button of template "(.*)"$/, async name => {
    await tpltSteps.clickTemplateContextDelete(name);
});

When(/^click the delete confirm button of template "(.*)"$/, async name => {
    await tpltSteps.clickTemplateDeleteConfirm(name);
});
