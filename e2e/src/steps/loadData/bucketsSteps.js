const { expect, assert } = require('chai');
const { By, until } = require('selenium-webdriver');

const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const bucketsTab = require(__srcdir + '/pages/loadData/bucketsTab.js');
const influxUtils = require(__srcdir + '/utils/influxUtils.js');

let durationsMap = new Map();
durationsMap.set('1h', '1 hour');
durationsMap.set('6h', '6 hours');
durationsMap.set('12h', '12 hours');
durationsMap.set('24h', '1 days');
durationsMap.set('48h', '2 days');
durationsMap.set('72h', '3 days');
durationsMap.set('7d', '7 days');
durationsMap.set('14d', '14 days');
durationsMap.set('30d', '30 days');
durationsMap.set('90d', '90 days');
durationsMap.set('1y', '365 days');

class bucketsSteps extends baseSteps {

    constructor(driver){
        super(driver);
        this.bucketsTab = new bucketsTab(driver);
    }

    async isLoaded(){
        await this.bucketsTab.isTabLoaded();
    }

    async verifyOrderByName(bucketNames){
        let namesArray = bucketNames.split(',');
        await this.bucketsTab.getBucketCards().then(async cards => {
            for( let i = 0; i < cards.length; i++){
                let cardName = await cards[i].findElement(By.xpath('.//span[contains(@class,\'cf-resource-name--text\')]/span'));
                let cardText = await cardName.getText();
                if(namesArray[i].toUpperCase() === 'DEFAULT'){
                    expect(cardText).to.equal(__defaultUser.bucket);
                }else {
                    expect(cardText).to.equal(namesArray[i]);
                }
            }
        });
    }

    async verifyBucketNotListedByName(name){
        await this.assertNotPresent(await bucketsTab.getBucketCardSelectorByName(name));
    }

    async clickCreateBucket(){
        await this.clickAndWait(await this.bucketsTab.getCreateBucketBtn()); //todo better wait
        /*await this.bucketsTab.getCreateBucketBtn().then(async button => {
            await button.click();
        }); */
    }

    async verifyCreateBucketPopup(){
        await this.assertVisible(await this.bucketsTab.getPopupContainer());
        await this.assertVisible(await this.bucketsTab.getPopupTitle());
        await this.assertVisible(await this.bucketsTab.getPopupInputName());
        await this.assertVisible(await this.bucketsTab.getPopupRetentionNever());
        await this.assertVisible(await this.bucketsTab.getPopupRetentionIntervals());
        await this.assertVisible(await this.bucketsTab.getPopupCancelButton());
        await this.assertVisible(await this.bucketsTab.getPopupDismissButton());
        await this.assertVisible(await this.bucketsTab.getPopupCreateButton());
    }

    async verifyEditBucketPopup(){
        await this.assertVisible(await this.bucketsTab.getPopupContainer());
        await this.assertVisible(await this.bucketsTab.getPopupTitle());
        await this.assertVisible(await this.bucketsTab.getPopupInputName());
        await this.assertVisible(await this.bucketsTab.getPopupRetentionNever());
        await this.assertVisible(await this.bucketsTab.getPopupRetentionIntervals());
        await this.assertVisible(await this.bucketsTab.getPopupCancelButton());
        await this.assertVisible(await this.bucketsTab.getPopupDismissButton());
        await this.assertVisible(await this.bucketsTab.getPopupSaveChanges());
    }

    async verifyCreateBucketPopupNotPresent(){
        await this.assertNotPresent(await bucketsTab.getPopupContainerSelector());
        await this.assertNotPresent(await bucketsTab.getPopupTitleSelector());
    }

    async verifyRPIntervalControlsNotPresent(){
        // N.B. Interval controls replaced with dropdown 2019-09-11
        //await this.assertNotPresent(await bucketsTab.getPopupRPIntervalControlsSelector());
        await this.assertNotPresent(await bucketsTab.getPopupRPDurationSelectorButtonSelector());
    }

    async verifyRPIntervalControlsPresent(){
        // N.B. Interval controls replaced with dropdown 2019-09-11
        //await this.assertVisible(await this.bucketsTab.getPopupRPIntevalControls());
        await this.assertVisible(await this.bucketsTab.getPopupRPDurationSelectorButton());
    }

    async clickPopupRPDurationSelectorButton(){
        await this.bucketsTab.getPopupRPDurationSelectorButton().then(async btn => {
            await btn.click().then(async  () => {
                await this.driver.sleep(500); //todo implement better wait
            });
        });
    }

    async verifyCreateBucketCreateButtonEnabled(enabled){
        await expect(await (await this.bucketsTab.getPopupCreateButton()).isEnabled()).to.equal(enabled);
    }

    async verifyNameInputEnabled(enabled){
        await expect(await (await this.bucketsTab.getPopupInputName()).isEnabled()).to.equal(enabled);
    }

    async verifyActiveRetentionPolicyButton(rp){
        await this.driver.findElement(By.css(`[data-testid=retention-${rp}--button]`))
            .then(async elem => {
                await elem.getAttribute('class').then( async elemClass => {
                    await expect(elemClass).to.include('active');
                });
            });
    }

    async verifyInactiveRetentionPolicyButton(rp){
        await this.driver.findElement(By.css(`[data-testid=retention-${rp}--button]`))
            .then(async elem => {
                await elem.getAttribute('class').then( async elemClass => {
                    await expect(elemClass).to.not.include('active');
                });
            });
    }

    async verifyPopupHelpText(text){
        await this.bucketsTab.getPopupHelpText().then(async elem => {
            await elem.getText().then(async elText => {
                expect(elText).to.include(text);
            });
        });
    }

    async clickRetentionPolicyButton(rp){
        await this.driver.findElement(By.css(`[data-testid=retention-${rp}--button]`))
            .then(async elem => {
                await elem.click();
            });
    }

    async dismissBucketPopup(){

        await this.bucketsTab.getPopupDismissButton().then(async btn => {
            await btn.click().then( async () => {
                await this.driver.wait(await this.bucketsTab
                    .getUntilElementNotPresent(bucketsTab.getPopupContainerSelector()));
                await this.driver.wait(await this.bucketsTab
                    .getUntilElementNotPresent(bucketsTab.getPopupTitleSelector()));
            });
        });
    }

    async cancelBucketPopup(){
        await this.bucketsTab.getPopupCancelButton().then(async btn => {
            await btn.click().then(async() => {
                await this.driver.wait(await this.bucketsTab
                    .getUntilElementNotPresent(bucketsTab.getPopupContainerSelector()));
                await this.driver.wait(await this.bucketsTab
                    .getUntilElementNotPresent(bucketsTab.getPopupTitleSelector()));
            });
        });
    }

    async setBucketName(name){
        await this.bucketsTab.getPopupInputName().then(async elem => {
            await elem.clear();
            await elem.sendKeys(name);
        });
    }

    async enterIntervalValue(amount, unit){
        switch(unit.toLowerCase()){
        case 'seconds':
        case 'second':
            await this.bucketsTab.getPopupRPSecondsInput().then(async elem => {
                await elem.sendKeys(amount);
            });
            break;
        case 'minutes':
        case 'minute':
            await this.bucketsTab.getPopupRPMinutesInput().then(async elem => {
                await elem.sendKeys(amount);
            });
            break;
        case 'hours':
        case 'hour':
            await this.bucketsTab.getPopupRPHoursInput().then(async elem => {
                await elem.sendKeys(amount);
            });
            break;
        case 'days':
        case 'day':
            await this.bucketsTab.getPopupRPDaysInput().then(async elem => {
                await elem.sendKeys(amount);
            });
            break;
        default:
            throw `unknown interval unit ${unit}`;
        }
    }

    //Replaces above 2019-09-11
    async clickRPSelectorValue(rp){
        await this.bucketsTab.getPopupRPDurationSelectorItem(rp).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(500); //todo better wait
            });
        });
    }

    async clearAllRetentionPolicyIntervals(){
        await this.bucketsTab.getPopupRPSecondsInput().then(async elem => {
            await elem.clear();
            await elem.sendKeys('0');
        });

        await this.bucketsTab.getPopupRPMinutesInput().then(async elem => {
            await elem.clear();
            await elem.sendKeys('0');
        });

        await this.bucketsTab.getPopupRPHoursInput().then(async elem => {
            await elem.clear();
            await elem.sendKeys('0');
        });

        await this.bucketsTab.getPopupRPDaysInput().then(async elem => {
            await elem.clear();
            await elem.sendKeys('0');
        });
    }

    async verifyIntervalValue(value, unit){
        switch(unit.toLowerCase()){
        case 'seconds':
        case 'second':
            await this.bucketsTab.getPopupRPSecondsInput().then(async elem => {
                await elem.getAttribute('value').then(async elVal => {
                    expect(parseInt(elVal)).to.equal(parseInt(value));
                });
            });
            break;
        case 'minutes':
        case 'minute':
            await this.bucketsTab.getPopupRPMinutesInput().then(async elem => {
                await elem.getAttribute('value').then(async elVal => {
                    expect(parseInt(elVal)).to.equal(parseInt(value));
                });
            });
            break;
        case 'hours':
        case 'hour':
            await this.bucketsTab.getPopupRPHoursInput().then(async elem => {
                await elem.getAttribute('value').then(async elVal => {
                    expect(parseInt(elVal)).to.equal(parseInt(value));
                });
            });
            break;
        case 'days':
        case 'day':
            await this.bucketsTab.getPopupRPDaysInput().then(async elem => {
                await elem.getAttribute('value').then(async elVal => {
                    expect(parseInt(elVal)).to.equal(parseInt(value));
                });
            });
            break;
        default:
            throw `unknown interval unit ${unit}`;
        }
    }

    async clickCreatePopupCreate(){
        await this.bucketsTab.getPopupCreateButton().then( async btn => {
            await btn.click();
            await this.driver.sleep(3000);
        });
    }

    async verifyFormErrorMessageContains(msg){
        await this.bucketsTab.getPopupFormError().then(async elem => {
            await elem.getText().then(async text => {
                expect(text).to.include(msg);
            });
        });
    }

    async verifyFormErrorMessageNotPresent(){
        await this.assertNotPresent(await bucketsTab.getPopupFormErrorSelector());
    }

    async verifyBucketInListByName(name){
        try{
            await this.bucketsTab.getBucketCardByName(name);
        }catch(err){
            assert.fail(`Failed to locate card named ${name} in current list`);
        }

        /*await this.bucketsTab.getBucketCards().then(async cards => {
            for(let i = 0; i < cards.length; i++){
                let crd_nm = await cards[i].findElement(By.xpath('.//span[contains(@class,\'cf-resource-name--text\')]/span'));
                if((await crd_nm.getText()) === name){
                    assert(true, `matched card with name ${name}`);
                    return;
                }
            }
            assert.fail(`Failed to locate card named ${name} in current list`);
        });*/
    }

    async verifyBucktNotInListByName(name){
        await this.assertNotPresent(await bucketsTab.getBucketCardSelectorByName(name));
    }

    async verifyBucketHasRetentionPolicy(name, rp){

        await this.driver.sleep(500); //todo - this is troubleshoot of occasional old value for rpText

        await this.bucketsTab.getBucketCardRetentionByName(name).then(async elem => {
            await elem.getText().then(async rpText => {
                if(rp.toLowerCase() === 'never'){
                    let rpPolicy = rpText.trim().toLowerCase().split(' ');
                    rpPolicy.shift();
                    await expect(rpPolicy[0]).to.equal('forever');
                }else{
                    await expect(rpText).to.include(durationsMap.get(rp));
                    //let policy = rp.trim().toLowerCase().split(' ');
                    //                    let rpPolicy = rpText.trim().toLowerCase().split(':');
                    //                    rpPolicy.shift(); //remover first 'Retenition: string'
                    //                    await expect(rpPolicy[0])
                    //                    for( let i = 0; i < policy.length; i += 2) {
                    //                        await expect(parseInt(policy[i])).to.equal(parseInt(rpPolicy[i]));
                    //                        await expect(policy[i+1]).to.equal(rpPolicy[i+1]);
                    //                    }
                }

            });
        });

        /*
        await this.bucketsTab.getBucketCards().then(async cards => {
            for(let i = 0; i < cards.length; i++){
                let crd_nm = await cards[i].findElement(By.xpath('.//span[contains(@class,\'cf-resource-name--text\')]/span'));
                if((await crd_nm.getText()) === name){
                    await cards[i].findElement(By.xpath('.//*[@data-testid=\'cf-resource-card--meta-item\']')).then(async elem => {
                       //await console.log("DEBUG cards[i] " + await elem.getText());
                        // rpText should be of form e.g. 'Retention: 28 days'
                       await elem.getText().then(async rpText => {
                           console.log("DEBUG rp #" + rp + "#");
                           console.log("DEBUG rpText #" + rpText + "#");
                           let policy = rp.trim().toLowerCase().split(' ');
                           let rpPolicy = rpText.trim().toLowerCase().split(' ');
                           rpPolicy.shift(); //remover first 'Retenition: string'
                           for( let i = 0; i < policy.length; i += 2) {
                               await expect(parseInt(policy[i])).to.equal(parseInt(rpPolicy[i]));
                               await expect(policy[i+1]).to.equal(rpPolicy[i+1]);
                           }
                       })
                    });
                    return;
                }
            }
            assert.fail(`Failed to locate card named ${name} in current list`);

        })*/
    }

    async clickOnBucketNamed(name){
        await this.clickAndWait(await this.bucketsTab.getBucketCardName(name),
            //N.B. popup sometimes slow to load
            async () => { await this.driver.sleep(1000);}); // todo better wait
    }

    async clickOnBucketSettings(name){
        await this.clickAndWait(await this.bucketsTab.getBucketCardSettingsByName(name));
    }

    async clickSaveChanges(){
        await this.bucketsTab.getPopupSaveChanges().then(async btn => {
            await btn.click();
        });
    }

    async setFilterValue(text){
        let cardCt = (await this.bucketsTab.getBucketCards()).length;
        console.log(`DEBUG cardCt ${cardCt}`);
        await this.bucketsTab.getFilterInput().then( async input => {
            await input.clear().then(async () => {
                await input.sendKeys(text).then( async () => {

                    await this.driver.sleep(500); //wait below seems to be blocking

                    /*await this.driver.wait(async () => {
                        return (await this.bucketsTab.getBucketCards()).length < cardCt;
                    }); */
                });
            });
        });
    }

    async clearFilterValue(){
        //let cardCt = (await this.bucketsTab.getBucketCards()).length;
        await this.bucketsTab.getFilterInput().then(async input => {
            await input.clear().then(async() => {
                await this.driver.sleep(500); //wait commented below seems to hang
                /*await this.driver.wait(async () => {
                    return (await this.bucketsTab.getBucketCards()).length > cardCt;
                }); */
            });
        });
    }

    async ensureNameSortOrder(order){
        await this.bucketsTab.getNameSorter().then(async elem => {
            if(!(await elem.getAttribute('title')).toLowerCase().includes(order.toLowerCase())){
                await elem.click().then(async () => {
                    await this.driver.wait(until.elementLocated(By.css((await bucketsTab.getNameSorterSelector()).selector)));
                });
            }
        });
    }

    async clickRetentionSort(){
        await this.clickAndWait(await this.bucketsTab.getPolicySorter());
    }

    async clickBucketsFilter(){
        await this.clickAndWait(await this.bucketsTab.getFilterInput());
    }

    async hoverOverCardNamed(name){
        await this.hoverOver(await this.bucketsTab.getBucketCardByName(name));
    }

    async verifyBucketCardDeleteNotPresent(name){
        //await this.driver.executeScript('arguments[0].blur()', await this.bucketsTab.getBucketCardDeleteByName(name));
        //await this.bucketsTab.getBucketCardByName('_tasks').then(async elem => {
        //  await elem.click().then(async () => {  //remove focus from list
        //    await this.driver.sleep(500); //fix later - losing patience
        await this.assertNotVisible(await this.bucketsTab.getBucketCardDeleteByName(name));
        // });
        //});
    }

    async verifyBucketCardPopoverVisible(name, toBeVisible){
        if(toBeVisible){
            await this.assertVisible(await this.bucketsTab.getBucketCardPopover());
        }else{
            await this.assertNotVisible(await this.bucketsTab.getBucketCardPopover());
        }
    }

    async verifyBucketCardPopover(present){
        if(present){
            await this.assertPresent(await bucketsTab.getPopoverSelector());
        }else{
            await this.assertNotPresent(await bucketsTab.getPopoverSelector());
        }
    }

    async clickBucketCardDelete(name){
        await this.clickAndWait(await this.bucketsTab.getBucketCardDeleteByName(name)); //todo  better wait
        /*await this.bucketsTab.getBucketCardDeleteByName(name).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.wait(until.elementIsVisible(
                    await this.bucketsTab.getBucketCardDeleteConfirmByName(name)));
            });
        });*/
    }

    async clickBucketCardDeleteConfirm(name){
        await this.bucketsTab.getBucketCardDeleteConfirmByName(name).then(async elem => {
            await elem.click().then(async () => {

                await this.driver.sleep(500); // todo - find better wait - however below is flakey

                // await this.driver.wait(until.stalenessOf(await this.bucketsTab.getBucketCardDeleteByName(name)));
            });
        });
    }

    async clickAddDataButtonOfCard(name){
        await this.bucketsTab.getBucketCardAddDataByName(name).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.wait(until.elementIsVisible(await this.bucketsTab.getBucketCardPopover()));
            });
        });
    }

    async clickPopoverItemForBucketCard(name, item){
        await this.bucketsTab.getBucketCardPopoverItemByName(name, item).then(async elem => {
            await elem.click().then( async () => {
                if(item.toLowerCase().includes('scrape metrics')) {
                    await this.driver.wait(until.elementIsVisible(await this.bucketsTab.getPopupOverlayContainer()));
                }else{ //line protocol and telegraf lead to wizard
                    await this.driver.wait(until.elementIsVisible(await this.bucketsTab.getWizardStepTitle()))
                        .catch(async err => {
                            console.log('Caught err' + err);
                            throw err;
                        });
                }
            });
        });
    }

    async clickPopoverItem(item){
        await this.clickAndWait(await this.bucketsTab.getPopoverItem(item));
    }

    async verifyLineProtocolWizardVisible(visibility){
        if(visibility){
            await this.assertVisible(await this.bucketsTab.getWizardStepTitle());
            //await this.assertVisible(await this.bucketsTab.getWizardStepSubTitle());
            await this.assertVisible(await this.bucketsTab.getWizardDragAndDrop());
            await this.assertVisible(await this.bucketsTab.getWizardRadioUploadFile());
            await this.assertVisible(await this.bucketsTab.getWizardRadioManual());
            await this.assertVisible(await this.bucketsTab.getWizardPrecisionDropdown());
            await this.assertVisible(await this.bucketsTab.getWizardContinueButton());
        }else{
            await this.assertNotPresent(await bucketsTab.getWizardStepTitleSelector());
            await this.assertNotPresent(await bucketsTab.getWizardStepSubTitleSelector());
            await this.assertNotPresent(await bucketsTab.getWizardContinueButtonSelector());
        }
    }

    async verifyDataPointsTextAreaVisible(visibility){
        if(visibility){
            await this.assertVisible(await this.bucketsTab.getWizardTextArea());
        }else{
            await this.assertNotVisible(await this.bucketsTab.getWizardTextArea());
        }
    }

    async clickRadioButton(name){
        if(name.toLowerCase().includes('manual')) {
            await this.bucketsTab.getWizardRadioManual().then(async elem => {
                await elem.click().then( async () => {
                    await this.driver.sleep(500); //todo better wait
                });
            });
        }else{
            await this.bucketsTab.getWizardRadioUploadFile().then(async elem => {
                await elem.click().then(async () => {
                    await this.driver.sleep(500); //todo better wait
                });
            });
        }
    }

    async enterLineProtocolDataPoints(count, value, start, mode){
        let samples = [];
        let dataPoints = [];
        let nowMillis = new Date().getTime();
        //line protocol i.e. myMeasurement,host=myHost testField="testData" 1556896326
        let intervals = await influxUtils.getIntervalMillis(count, start);
        let startMillis = nowMillis - intervals.full;
        switch(mode.toLowerCase()){
        case 'fibonacci':
            samples = await influxUtils.genFibonacciValues(count);
            break;
        default:
            throw `Unhandled mode ${mode}`;
        }
        for(let i = 0; i < samples.length; i++){
            console.log(`${mode},test=bucketSteps ${value}=${samples[i]} ${startMillis + (intervals.step * i)}\n`);
            dataPoints.push(`${mode},test=bucketSteps ${value}=${samples[i]} ${startMillis + (intervals.step * i)}\n`);
        }
        await this.bucketsTab.getWizardTextArea().then(async elem => {
            dataPoints.forEach(async point => {
                await elem.sendKeys(point);
            });
        });

    }

    async enterLineProtocolRawData(data){
        await this.bucketsTab.getWizardTextArea().then(async elem => {
            await elem.sendKeys(data).then(async () => {
                await this.driver.sleep(500); // todo better wait
            });
        });
    }

    async clickLineProtocolPrecisionDropdown(){
        await this.bucketsTab.getWizardPrecisionDropdown().then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(500); //todo better wait
            });
        });
    }

    async clickLineProtocolContinue(){
        await this.bucketsTab.getWizardContinueButton().then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(500); //todo better wait
            });
        });
    }

    async clickLineProtocolPrecisionItem(prec){
        await this.bucketsTab.getWizardDropdownPrecisionItem(prec).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(500); //todo better wait
            });
        });
    }

    async verifyLineProtocolWizardSecondStep(){
        await this.assertVisible(await this.bucketsTab.getWizardSparkleSpinner());
        await this.assertVisible(await this.bucketsTab.getWizardStepStateText());
        await this.assertVisible(await this.bucketsTab.getWizardFinishButton());

    }

    async verifyWizardStepStatusMessage(msg){
        await this.bucketsTab.getWizardStepStateText().then(async elem => {
            await elem.getText().then(async elText => {
                expect(elText).to.equal(msg);
            });
            if(msg.toLowerCase().includes('success')) {
                await elem.getCssValue('color').then(async color => {
                    expect(color).to.equal('rgba(190, 194, 204, 1)');
                });
            }
        });
    }

    async verifyWizardStepStatusMessageContains(msg){
        await this.bucketsTab.getWizardStepStateText().then(async elem => {
            await elem.getText().then(async elText => {
                expect(elText).to.include(msg);
            });
            if(msg.toLowerCase().includes('success')) {
                await elem.getCssValue('color').then(async color => {
                    expect(color).to.equal('rgba(78, 216, 160, 1)');
                });
            }else{
                await elem.getCssValue('color').then(async color => {
                    expect(color).to.equal('rgba(255, 133, 100, 1)');
                });
            }
        });

    }

    async clickLineProtocolFinish(){
        await this.bucketsTab.getWizardFinishButton().then( async button => {
            await button.click().then(async () => {
                await this.driver.sleep(500); //todo better wait
            });
        });
    }

    async clickDataWizardPreviousButton(){
        await this.clickAndWait(await this.bucketsTab.getDataWizardPreviousButton());
    }


}

module.exports = bucketsSteps;

