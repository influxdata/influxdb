import { Given, Then, When } from 'cucumber';
const bucketsSteps = require(__srcdir + '/steps/loadData/bucketsSteps.js');
const influxUtils = require(__srcdir + '/utils/influxUtils.js');

let bktTabSteps = new bucketsSteps(__wdriver);

Then(/^the buckets are sorted as "(.*)"$/, async (bucketNames) => {
    await bktTabSteps.verifyOrderByName(bucketNames);
});

Then(/^the bucket "(.*)" is not in the list$/, async (name) => {
    await bktTabSteps.verifyBucketNotListedByName(name);
});

When(/^click the Create Bucket button$/, async () => {
    await bktTabSteps.clickCreateBucket();
});

Then(/^the Create Bucket Popup is loaded$/, async () => {
    await bktTabSteps.verifyCreateBucketPopup();
});

Then(/^the Create button of Create Bucket Popup is disabled$/, async () => {
    await bktTabSteps.verifyCreateBucketCreateButtonEnabled(false);
});

Then(/^the Create button of Create Bucket Popup is enabled$/, async () => {
    await bktTabSteps.verifyCreateBucketCreateButtonEnabled(true);
});

Then(/^the Retention Policy radio button "(.*)" is active$/, async (rp) => {
    await bktTabSteps.verifyActiveRetentionPolicyButton(rp);
});

Then(/^the Retention Policy radio button "(.*)" is inactive$/, async (rp) => {
    await bktTabSteps.verifyInactiveRetentionPolicyButton(rp);
});


//Waiting for dismissal can take a few secs
When(/^dismiss the Create Bucket Popup$/, {timeout: 2 * 5000}, async () => {
    await bktTabSteps.dismissBucketPopup();
});

When(/^dismiss the Edit Bucket Popup$/, {timeout: 2 * 5000}, async () => {
    await bktTabSteps.dismissBucketPopup();
});

// Should be not present - removed from DOM
Then(/^the Create Bucket Popup is not present$/, {timeout: 2 * 5000}, async () => {
    await bktTabSteps.verifyCreateBucketPopupNotPresent();
});

Then(/^the Edit Bucket Popup is not present$/, {timeout: 2 * 5000}, async () => {
    await bktTabSteps.verifyCreateBucketPopupNotPresent();
});

//Waiting for dismissal can take a few secs
When(/^cancel the Create Bucket Popup$/, {timeout: 2 * 5000}, async () => {
    await bktTabSteps.cancelBucketPopup();
});

When(/^cancel the Edit Bucket Popup$/, {timeout: 2 * 5000}, async () => {
    await bktTabSteps.cancelBucketPopup();
});

Then(/^the Retention Policy intervals controls are not present$/, {timeout: 2 * 5000}, async () => {
    await bktTabSteps.verifyRPIntervalControlsNotPresent();
});

Then(/^the Retention Policy intervals controls are present$/, async () => {
    await bktTabSteps.verifyRPIntervalControlsPresent();
});

When(/^click the Retention Policy "(.*)" button$/, async(rp) => {
    await bktTabSteps.clickRetentionPolicyButton(rp);
});

When(/^input the name of the bucket as "(.*)"$/, async (name) => {
    await bktTabSteps.setBucketName(name);
});

When(/^clear all Retention Policy interval controls$/, async () => {
    await bktTabSteps.clearAllRetentionPolicyIntervals();
});

When(/^enter "(.*)" into the Retention Policy "(.*)" control$/, async(amount, unit) => {
    await bktTabSteps.enterIntervalValue(amount, unit);
});

Then(/^the Retention Policy "(.*)" control contains the value "(.*)"$/, async(unit, value) => {
    await bktTabSteps.verifyIntervalValue(value, unit);
});


When(/^set the retention policy of the bucket as "(.*)"$/, async (rp) => {

    if(rp.toLowerCase() === 'never'){
        await bktTabSteps.clickRetentionPolicyButton('never');
    }else{
        //rp = rp.trim();
        //let policy = rp.split(" ");
        await bktTabSteps.clickRetentionPolicyButton('intervals');
        await bktTabSteps.clickPopupRPDurationSelectorButton();
        await bktTabSteps.clickRPSelectorValue(rp);
        //await bktTabSteps.enterIntervalValue(policy[0], policy[1]);
    }
});

When(/^click the Create Bucket popup Create button$/, async () => {
    await bktTabSteps.clickCreatePopupCreate();
});

Then(/^the Retention Policy warning message contains "(.*)"$/, async (msg) => {
    await bktTabSteps.verifyFormErrorMessageContains(msg);
});

Then(/^the Retention Policy warning message has disappeared$/, {timeout: 2 * 5000}, async () => {
    await bktTabSteps.verifyFormErrorMessageNotPresent();
});

Then(/^the bucket named "(.*)" is in the list$/, async (name) => {
    await bktTabSteps.verifyBucketInListByName(name);
});

Then(/^the bucket card named "(.*)" is not in the list$/, async (name) => {
    await bktTabSteps.verifyBucktNotInListByName(name);
});


Then(/^the bucket named "(.*)" has a Retention Policy of "(.*)"$/, async (name, rp) => {
    await bktTabSteps.verifyBucketHasRetentionPolicy(name, rp);
});

When(/^click on the bucket named "(.*)"$/, async (name) => {
    await bktTabSteps.clickOnBucketNamed(name);
});

When(/^click on settings for bucket named "(.*)"$/, async (name) => {
    await bktTabSteps.clickOnBucketSettings(name);
});

Then(/^the Edit Bucket popup is loaded$/, async () => {
    await bktTabSteps.verifyEditBucketPopup();
});

Then(/^the name edit textbox of the Edit Bucket popup is disabled$/, async() => {
    await bktTabSteps.verifyNameInputEnabled(false);
});

Then(/^the form help text contains "(.*)"$/, async (text) => {
    await bktTabSteps.verifyPopupHelpText(text);
});

When(/^click Edit Bucket Popup Save Changes$/, async () => {
    await bktTabSteps.clickSaveChanges();
});

When(/^enter "(.*)" in the Buckets filter field$/, {timeout: 10000},  async (text) => {
    await bktTabSteps.setFilterValue(text);
});

When(/^clear the Buckets filter field$/, async () => {
    await bktTabSteps.clearFilterValue();
});

Given(/^ensure buckets name sort order "(.*)"$/,{timeout: 2 * 5000}, async (order) => {
    await bktTabSteps.ensureNameSortOrder(order);
});

When(/^click buckets sort by retention policy$/, async () => {
    await bktTabSteps.clickRetentionSort();
});

When(/^click the buckets page title$/, async () => {
    await bktTabSteps.clickPageTitle();
});

//need to move focus from list sometimes
When(/^click buckets filter$/, async () => {
    await bktTabSteps.clickBucketsFilter();
});

When(/^hover over bucket card named "(.*)"$/,{timeout: 2 * 5000}, async (name) => {
    await bktTabSteps.hoverOverCardNamed((name === 'DEFAULT') ? __defaultUser.bucket : name);
    await bktTabSteps.driver.sleep(5000);
});

Then(/^the delete button of the card named "(.*)" is not present$/, {timeout: 2 * 5000}, async (name) => {
    await bktTabSteps.verifyBucketCardDeleteNotPresent(name);
});

When(/^click the delete button of the card named "(.*)"$/, {timeout: 10000}, async (name) => {
    await bktTabSteps.clickBucketCardDelete(name);
});

When(/^click the confirm delete button of the card named "(.*)"$/, async (name) => {
    await bktTabSteps.clickBucketCardDeleteConfirm(name);
});

When(/^click add data button for bucket "(.*)"$/, async (name) => {
    await bktTabSteps.clickAddDataButtonOfCard((name === 'DEFAULT') ? __defaultUser.bucket : name);
});

Then(/^the add data popover for the bucket "(.*)" is not visible$/, async (name) => {
    await bktTabSteps.verifyBucketCardPopoverVisible((name === 'DEFAULT') ? __defaultUser.bucket : name, false);
});

Then(/^the add data popover is not present$/, async () => {
    await bktTabSteps.verifyBucketCardPopover(false);
});


Then(/^the add data popover for the bucket "(.*)" is visible$/, async (name) => {
    await bktTabSteps.verifyBucketCardPopoverVisible((name === 'DEFAULT') ? __defaultUser.bucket : name, true);
});

When(/^click the popover item "(.*)" for the bucket "(.*)"$/, async (item, name) => {
    await bktTabSteps.clickPopoverItemForBucketCard((name === 'DEFAULT') ? __defaultUser.bucket : name, item);
});

When(/^click bucket card popover item "(.*)"$/, async item => {
    await bktTabSteps.clickPopoverItem(item);
});

Then(/^the first page of the Line Protocol Wizard is loaded$/, async () => {
    await bktTabSteps.verifyLineProtocolWizardVisible(true);
});

When(/^click radio button "(.*)"$/, async (name) => {
    await bktTabSteps.clickRadioButton(name);
});

Then(/^the data point text area is visible$/, async () => {
    await bktTabSteps.verifyDataPointsTextAreaVisible(true);
});

When(/^enter "(.*)" datapoints with value named "(.*)" starting at "(.*)" with "(.*)" data of type "(.*)" and prec "(.*)"$/,
    async (count, value, start, mode, type, prec) => {
        await bktTabSteps.enterLineProtocolDataPoints(count, value, start, mode, type, prec);
        await bktTabSteps.driver.sleep(3000);
    });


When(/^enter "(.*)" into the line protocol text area$/, async data => {
    await bktTabSteps.enterLineProtocolRawData(data);
});

When(/^click the Line Protocol wizard precision dropdown$/, async () => {
    await bktTabSteps.clickLineProtocolPrecisionDropdown();
});

When(/^click the Line Protocol wizard continue button$/, async () => {
    await bktTabSteps.clickLineProtocolContinue();
});

When(/^click the Line Protocol wizard write button$/, async () => {
   await bktTabSteps.clickLineProtocolWrite();
});

When(/^click the line Protocol wizard precision "(.*)"$/, async (prec) => {
    await bktTabSteps.clickLineProtocolPrecisionItem(prec);
});

Then(/^the line Protocol wizard second step opens$/, async() => {
    await bktTabSteps.verifyLineProtocolWizardSecondStep();
});

Then(/^the Line Protocol wizard step status message is "(.*)"$/, async msg => {
    await bktTabSteps.verifyWizardStepStatusMessage(msg);
});

Then(/^the Line Protocol wizard step status message contains "(.*)"$/, async msg => {
    await bktTabSteps.verifyWizardStepStatusMessageContains(msg);
});

When(/^click the Line Protocol wizard finish button$/, async () => {
    await bktTabSteps.clickLineProtocolFinish();
});

When(/^click the Line Protocol wizard close button$/, async () => {
   await bktTabSteps.clickLineProtocolClose();
});

When(/^click the Line Protocol wizard cancel button$/, async () => {
   await bktTabSteps.clickLineProtocolCancel();
});

When(/^click the Line Protocol wizard close icon$/, async () => {
   await bktTabSteps.clickLineProtocolCancelIcon();
});

Then(/^the line Protocol wizard is not present$/, {timeout: 2 * 5000}, async () => {
    await bktTabSteps.verifyLineProtocolWizardVisible(false);
});

Then(/^the bucket "(.*)" for user "(.*)" contains "(.*)" datapoints of "(.*)" data with value named "(.*)" starting at "(.*)"$/,
    async (bucket, user, count, mode, value, start) => {

        await bktTabSteps.verifyBucketContains((bucket === 'DEFAULT') ? __defaultUser.bucket : bucket,
            (user === 'DEFAULT')? __defaultUser: await influxUtils.getUser(user),
            count, mode, value, start);
    });

Then(/^the bucket "(.*)" for user "(.*)" contains:$/, async (bucket, userName, def) => {
    await bktTabSteps.verifyBucketContainsByDef(bucket,userName, def);
});

When(/^add the file "(.*)" to the Line Protocol Wizard file upload$/, async filePath => {
    await bktTabSteps.setFileUpload(filePath);
});

When(/^click the bucket data wizard previous button$/, async () => {
    await bktTabSteps.clickDataWizardPreviousButton();
});

