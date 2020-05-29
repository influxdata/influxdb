import { Then, When } from 'cucumber';

const telegrafsSteps = require(__srcdir + '/steps/loadData/telegrafsSteps.js');

let teleTabSteps = new telegrafsSteps(__wdriver);

Then(/^there is a telegraf card for "(.*)"$/, async name => {

    await teleTabSteps.verifyTelegrafCardByName(name);

});

Then(/^the telegrafs tab is loaded$/, async () => {
    await teleTabSteps.verifyTelegrafTabLoaded(true);
});

When(/^click the create Telegraf button empty$/, async () => {
    await teleTabSteps.clickCreateTelegrafButtonEmpty();
});

When(/^click telegraf card "(.*)"$/, async (name) => {
    await teleTabSteps.clickTelegrafCard(name);
});

When(/^click the create Telegraf button in header$/, async () => {
    await teleTabSteps.clickCreateTelegrafButtonInHeader();
});

Then(/^the Create Telegraf Wizard is loaded$/, { timeout: 10000 }, async () => {
    await teleTabSteps.verifyWizardLoadedP1();
});

When(/^dismiss the Create Telegraf Wizard$/, async () => {
    await teleTabSteps.dismissPopup();
});

Then(/^the Create Telegraf Wizard is no longer present$/, { timeout: 10000 },  async () => {
    await teleTabSteps.verifyCreateTelegrafWizardNotPresent();
});

When(/^click the select bucket dropdown in the Create Telegraf Wizard$/, async () => {
    await teleTabSteps.clickCreateConfigBucketDropdown();
});

When(/^click the bucket item "(.*)" in the Create Telegraf Wizard$/, async item => {
    await teleTabSteps.clickCreateConfigBucketDropdownItem((item === 'DEFAULT') ? __defaultUser.bucket : item);
});

When(/^click the plugin tile "(.*)" in the Create Telegraf Wizard$/, async plugin => {
    await teleTabSteps.clickCreateConfigPluginTile(plugin);
});

When(/^clear the create Telegraf Wizard plugin filter$/, async () => {
    await teleTabSteps.clearWizardPluginFilter();
});

When(/^enter the value "(.*)" in create Telegraf Wizard filter plugins$/, async value => {
    await teleTabSteps.enterValueIntoPluginsFilter(value);
});

Then(/^the Create Telegraf wizard plugin tile "(.*)" is visible$/, async plugin => {
    await teleTabSteps.verifyCreateWizardPluginTileVisible(plugin);
});

Then(/^the Create Telegraf wizard plugin tile "(.*)" is not present$/, async plugin => {
    await teleTabSteps.verifyCreateWizardPluginTileNotPresent(plugin);
});

Then(/^the create Telegraf Wizard second step is loaded$/, {timeout: 10000},  async () => {
    await teleTabSteps.verifyCreateWizardStep2Loaded();
});

Then(/^the create Telegraf plugins sidebar contains "(.*)"$/,
    async plugins => {
        await teleTabSteps.verifyCreateWizardStep2PluginsList(plugins.toLowerCase());
    });

Then(/^the create Telegraf plugin sidebar "(.*)" item is in state "(.*)"$/, async (plugin, state) => {
    await teleTabSteps.verifyCreateWizardPluginState(plugin, state);
});

When(/^click the create Telegraf plugin sidebar "(.*)" item$/, async plugin => {
    await teleTabSteps.clickCreateWizardPluginItem(plugin);
});

Then(/^the create Telegraf edit plugin "(.*)" step is loaded$/, {timeout: 10000}, async plugin => {
    await teleTabSteps.verifyEditPluginStepLoaded(plugin);
});

Then(/^the Create Telegraf wizard plugin tile "(.*)" is selected$/, async plugin => {
    await teleTabSteps.verifyCreateWizardPluginTileSelected(plugin);
});

Then(/^the Create Telegraf wizard plugin tile "(.*)" is not selected$/, async plugin => {
    await teleTabSteps.verifyCreateWizardPluginTileNotSelected(plugin);
});

When(/^enter the values (.*) into the fields (.*)$/, async (values, fields) => {
    await teleTabSteps.enterValuesIntoFields(values, fields);
});

When(/^click the NGINX configuration add button$/, async () => {
    await teleTabSteps.clickNGINXConfigAddUrlButton();
});

Then(/^the NGINX configuration URLs list contains "(.*)" items$/, async ct => {
    await teleTabSteps.verifyNGINXConfUrlsListSize(ct);
});

Then(/^the NGINX configuration URLs list is empty$/, async () => {
    await teleTabSteps.verifyNGINXConfUrlsListEmpty();
});

When(/^click delete for the first NGINX configuration URL$/, async () => {
    await teleTabSteps.clickNGINXConfUrlsFirstDelete();
});

When(/^click confirm delete of NGINX configuration URL$/, async () => {
    await teleTabSteps.clickNGINXConfUrlDeleteConfirm();
});

Then(/^verify the edit plugin error notification with message "(.*)"$/, async msg => {
    await teleTabSteps.verifyEditPluginErrorMessage(msg);
});

When(/^clear the create Telegraf edit plugin fields (.*)$/, async fields => {
    await teleTabSteps.clearCreateTelegrafPluginFields(fields);
});

When(/^enter the name "(.*)" in the Create Telegraf Wizard$/, async name => {
    await teleTabSteps.enterTelegrafWizardName(name);
});

When(/^enter the description "(.*)" in the Create Telegraf Wizard$/, async descr => {
    await teleTabSteps.enterTelegrafWizardDescr(descr);
});

When(/^click the Create Telegraf Wizard finish button$/, async () => {
    await teleTabSteps.clickPopupWizardFinish();
});

Then(/^the create Telegraf Wizard final step is loaded$/, async () => {
    await teleTabSteps.verifyCreateWizardStep3Loaded();
});

Then(/^the bucket of the telegraf card "(.*)" is "(.*)"$/, async (name, bucket) => {
    await teleTabSteps.verifyBucketForTelegrafCard(name, (bucket === 'DEFAULT') ? __defaultUser.bucket : bucket);
});

Then(/^the description of the telegraf card "(.*)" is "(.*)"$/, async (name, descr) => {
    await teleTabSteps.verifyDescriptionForTelegrafCard(name, descr);
});

Then(/^the telegraf sort order is "(.*)"$/, async order => {
    await teleTabSteps.verifyTelegrafCardSortOrder(order);
});

When(/^click the telegraf sort by name button$/, async () => {
    await teleTabSteps.clickTelegrafSortByName();
});

When(/^click the telegraf sort by bucket button$/, async () => {
    await teleTabSteps.clickTelegrafSortByBucket();
});

When(/^enter the value "(.*)" into the Telegrafs filter$/, async value => {
    await teleTabSteps.enterTelegrafsFilterValue(value);
});

When(/^clear the Telegrafs filter$/, async () => {
    await teleTabSteps.clearTelegrafsFilter();
});

When(/^click on setup instructions for the telegraf card "(.*)"$/, async card => {
    await teleTabSteps.clickSetupInstructionsForCard(card);
});

Then(/^the telegraf setup instruction popup is loaded$/, async () => {
    await teleTabSteps.verifyTelegrafSetupPopup();
});

When(/^click on the name of the telegraf card "(.*)"$/, async name => {
    await teleTabSteps.clickTelegrafCardNamed(name);
});

When(/^hover over the name of the telegraf card "(.*)"$/, async name => {
    await teleTabSteps.hoverOverTelegrafCardName(name);
});

Then(/^the telegraf configuration popup for "(.*)" is loaded$/, async name => {
    await teleTabSteps.verifyTelegrafConfigPopup(name);
});

When(/^click the name edit icon of the telegraf card "(.*)"$/, async name => {
    await teleTabSteps.clickNameEditIconOfTelegrafCard(name);
});

When(/^clear the name input of the telegraf card "(.*)"$/, async name => {
    await teleTabSteps.clearTelegrafCardNameInput(name);
});

When(/^clear the desrciption input of the telegraf card "(.*)"$/, async card => {
    await teleTabSteps.clearTelegrafCardDescrInput(card);
});

When(/^set the name input of the telegraf card "(.*)" to "(.*)"$/, async (oldName, newName) => {
    await teleTabSteps.setNameInputOfTelegrafCard(oldName, newName);
});

When(/^set the description input of the telegraf card "(.*)" to "(.*)"$/, async (name, descr) => {
    await teleTabSteps.setDescriptionInputOfTelegrafCard(name, descr);
});

Then(/^the Telegraf Card "(.*)" can no longer be found$/, async name => {
    await teleTabSteps.verifyTelegrafCardNotPresent(name);
});

Then(/^the telegraf cards "(.*)" are no longer present$/, {timeout: 20000}, async names => {
    let nameArr = names.split(',');
    for(let i = 0; i < nameArr.length; i++){
        await teleTabSteps.verifyTelegrafCardNotPresent(nameArr[i]);
    }
});

When(/^hover over the description of the telegraf Card "(.*)"$/, async card => {
    await teleTabSteps.hoverOverTelegrafCardDescription(card);
});

When(/^click the description edit icon of the telegraf card "(.*)"$/, async card => {
    await teleTabSteps.clickDescrEditIconOfTelegrafCard(card);
});

When(/^hover over telegraf card "(.*)"$/, async name => {
    await teleTabSteps.hoverOverTelegrafCard(name);
});

When(/^click delete for telegraf card "(.*)"$/, async name => {
    await teleTabSteps.clickTelegrafCardDelete(name);
});

When(/^click delete confirm for telegraf card "(.*)"$/, async name => {
    await teleTabSteps.clickTelegrafCardDeleteConfirm(name);
});

When(/^click Add Label for Telegraf Card "(.*)"$/, async name => {
    await teleTabSteps.clickTelegrafCardAddLabel(name);
});

Then(/^the Label Popup for the Telegraf Card "(.*)" is not present$/, async name => {
    await teleTabSteps.verifyTelegrafCardLabelPopupNotPresent(name);
});

Then(/^the Label Popup for the Telegraf Card "(.*)" is visible$/, async name => {
    await teleTabSteps.verifyTelegrafCardLabelPopupIsVisible(name);
});

Then(/^the item "(.*)" is in the Telegraf Card "(.*)" label select list$/, async (item, name) => {
    await teleTabSteps.verifyTelegrafCardLabelPopupSelectItem(name, item);
});

Then(/^the item "(.*)" is NOT in the Telegraf Card "(.*)" label select list$/, async (item, name) => {
    await teleTabSteps.verifyTelegrafCardLabelPopupSelectItemNotPresent(name, item);
});

When(/^filter the Telegraf Card "(.*)" label list with "(.*)"$/, async (name, term ) => {
    await teleTabSteps.filterTelegrafCardLabeList(name, term);
});

When(/^enter the value "(.*)" into the Telegraf Card "(.*)" label filter$/, async (term, name) => {
    await teleTabSteps.enterTermIntoTelegrafCardLabelFilter(name, term);
});

When(/^clear the label filter of the Telegraf Card "(.*)"$/, async name => {
    await teleTabSteps.clearTelegrafCardLabelFilter(name);
});

When(/^click the item "(.*)" is in the Telegraf Card "(.*)" label select list$/, async (item, name) => {
    await teleTabSteps.clickTelegrafCardLabelPopupSelectItem(name, item);
});

Then(/^there is a label pill "(.*)" for the Telegraf Card "(.*)"$/, async (item, name) => {
    await teleTabSteps.verifyTelegrafCardLabelPillIsVisible(name, item);
});

Then(/^the label select list for "(.*)" shows the empty state message$/, async name => {
    await teleTabSteps.verifyTelegrafCardLabelListEmptyMsg(name);
});

When(/^hover over the label pill "(.*)" for the Telegraf Card "(.*)"$/, async (label, name) => {
    await teleTabSteps.hoverTelegrafCardLabelPill(name, label);
});

When(/^click delete the label pill "(.*)" for the Telegraf Card "(.*)"$/, async (label, name) => {
    await teleTabSteps.clickTelegrafCardLabelPillDelete(name, label);
});

Then(/^the label pill "(.*)" for the Telegraf Card "(.*)" is NOT present$/, async (label, name) => {
    await teleTabSteps.verifyTelegrafCardLabelPillNotPresent(name, label);
});
