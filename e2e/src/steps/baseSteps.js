const fs = require('fs');
const chai = require('chai');
chai.use(require('chai-match'));

const expect = require('chai').expect;
const assert = require('chai').assert;

const { By, Key, until, logging } = require('selenium-webdriver');
const influxUtils = require(__srcdir + '/utils/influxUtils.js');

const basePage = require (__srcdir + '/pages/basePage.js');

const keyMap = {'enter': Key.ENTER,
    'tab': Key.TAB,
    'backspace': Key.BACK_SPACE,
    'bksp': Key.BACK_SPACE,
    'space': Key.SPACE,
    'escape': Key.ESCAPE,
    'esc': Key.ESCAPE,
    'ctrl': Key.CONTROL,
    'end': Key.END,
    'shift': Key.SHIFT,
    'del': Key.DELETE,
    'alft': Key.ARROW_LEFT,
    'arght': Key.ARROW_RIGHT,
    'aup': Key.ARROW_UP,
    'adown': Key.ARROW_DOWN
};


class baseSteps{
    constructor(driver){
        this.driver = driver;
        this.basePage = new basePage(driver);
    }

    async delay(timeout){
        await this.driver.sleep(timeout);
    }

    async clearBrowserLocalStorage(){
        await this.driver.executeScript('window.localStorage.clear()');
    }

    /* async open(url){
        await this.driver.get(url);
    } */

    async openBase(){
        await this.driver.get( `${__config.protocol}://${__config.host}:${__config.port}/`);
        await this.driver.wait(function(driver = this.driver) {
            return driver.executeScript('return document.readyState').then(function(readyState) {
                return readyState === 'complete';
            });
        });

    }

    async openContext(ctx){
        await this.driver.get( `${__config.protocol}://${__config.host}:${__config.port}/` + ctx);
        await this.driver.wait(function(driver = this.driver) {
            return driver.executeScript('return document.readyState').then(function(readyState) {
                return readyState === 'complete';
            });
        });
    }

    async waitForPageLoad(){
        await this.driver.wait(function(driver = this.driver) {
            return driver.executeScript('return document.readyState').then(function(readyState) {
                return readyState === 'complete';
            });
        });
    }

    async isNotificationMessage(message){
        //wait a second for all notifications to load
        await this.driver.sleep(1000);

        await this.basePage.getNoficicationSuccessMsgs().then(async elems => {
            let match = false;

            for(var i = 0; i < elems.length; i++){
                if(await elems[i].getText() === message){
                    match = true;
                    break;
                }
            }

            assert(match, `Failed to find "${message}" in notifications`);
        });
    }

    async containsNotificationText(text){
        await this.basePage.getNoficicationSuccessMsgs().then(async elems => {
            let match = false;

            for(var i = 0; i < elems.length; i++){
                if((await elems[i].getText()).includes(text)){
                    match = true;
                    break;
                }
            }

            assert(match, `Failed to find notification containing "${text}"`);
        });
    }

    async containsPrimaryNotificationText(text){
        await this.basePage.getNotificationPrimaryMsgs().then(async elems => {
            let match = false;

            for(var i = 0; i < elems.length; i++){
                if((await elems[i].getText()).includes(text)){
                    match = true;
                    break;
                }
            }

            assert(match, `Failed to find notification containing "${text}"`);
        });
    }

    async containsErrorNotificationText(text){
        await this.basePage.getNotificationErrorMsgs().then(async elems => {
            let match = false;

            for(var i = 0; i < elems.length; i++){
                if((await elems[i].getText()).includes(text)){
                    match = true;
                    break;
                }
            }

            assert(match, `Failed to find error notification containing "${text}"`);
        });
    }

    async closeAllNotifications(){
        await this.driver.sleep(500); //might not be loaded - todo better wait
        await this.basePage.getNotificationCloseButtons().then(async btns => {
            await btns.forEach(async(btn) => {
                await btn.click().catch(async e => {
                    //Ignore StaleElements - message likely already closed itself
                    if(e.name !== 'StaleElementReferenceError'){
                        throw e;
                    }
                });
            });
        });
        await this.driver.sleep(500); //make sure are closed - todo better wait

    }

    async hoverOver(element){
        let actions = await this.driver.actions({bridge: true});
        await actions.move({origin: element}).perform().then(async () => {
            //            console.log("DEBUG hover success ");
            //            await this.driver.sleep(1000)
            //console.log("SUCCESS " + resp)
        }).catch( err => {
            console.log('ERR ' + err);
            throw(err); // rethrow to cucumber - else error not flagged and step is success
        });
    }

    async verifyElementErrorMessage(msg, equal = true){
        await this.basePage.getPopupFormElementError().then(async elem => {
            await elem.getText().then(async elText => {
                if(equal) {
                    expect(elText).to.equal(msg);
                }else{
                    expect(elText).to.include(msg);
                }
            });
        });
    }

    async verifyNoElementErrorMessage(){
        await this.assertNotPresent(basePage.getPopupFormElementErrorSelector());
    }

    async verifyInputErrorIcon(){
        await this.basePage.getFormInputErrors(async elems => {
            expect(elems.length).to.be.above(0);
        });
    }

    async verifyNoFormInputErrorIcon(){
        await this.assertNotPresent(basePage.getFormInputErrorSelector());
    }

    async assertVisible(element){
        try {
            await expect(await element.isDisplayed()).to.equal(true);
        }catch(err){
            console.log('Assert Visible failed: ' + err + '/n waiting for ' + JSON.stringify(element));
            throw err;
        }
    }

    async assertNotVisible(element){
        await expect(await element.isDisplayed()).to.equal(false);
        //await expect(await element.isDisplayed().catch(async err => { console.log("DEBUG err " + err); throw err;})).to.equal(false).catch( async err => {
        //    console.log("assertNotVisible Error: " + await element.getCssValue())
        //    throw(err);
        //});
    } 

    //selector type should be {type, selector}
    async assertNotPresent(selector){
        switch(selector.type){
        case 'css':
            await this.driver.findElements(By.css(selector.selector)).then(async elems => {
                await expect(elems.length).to.equal(0);
            }).catch(async err => {
                err += ' expected ' + JSON.stringify(selector) + ' to not be present';
                throw err;
            });
            break;
        case 'xpath':
            await this.driver.findElements(By.xpath(selector.selector)).then(async elems => {
                await expect(elems.length).to.equal(0);
            }).catch(async err => {
                err.message += ' expected ' + selector + ' to not be present';
                throw err;
            });
            break;
        default:
            throw `Unknown selector type ${selector}`;
        }
    }

    async assertPresent(selector){
        switch(selector.type){
        case 'css':
            await this.driver.findElements(By.css(selector.selector)).then(async elems => {
                await expect(elems.length).to.be.above(0);
            }).catch(async err => {
                err += ' expected ' + JSON.stringify(selector) + ' to not be present';
                throw err;
            });
            break;
        case 'xpath':
            await this.driver.findElements(By.xpath(selector.selector)).then(async elems => {
                await expect(elems.length).to.be.above(0);
            }).catch(async err => {
                err.message += ' expected ' + selector + ' to not be present';
                throw err;
            });
            break;
        default:
            throw `Unknown selector type ${selector}`;
        }
    }

    async isPresent(selector){
        switch(selector.type){
        case 'css':
            return await this.driver.findElements(By.css(selector.selector)).then(async elems => {
                return elems.length > 0;
            }).catch(async err => {
                err += ' expected ' + JSON.stringify(selector) + ' to not be present';
                throw err;
            });
        case 'xpath':
            return await this.driver.findElements(By.xpath(selector.selector)).then(async elems => {
                return elems.length > 0;
            }).catch(async err => {
                err.message += ' expected ' + selector + ' to not be present';
                throw err;
            });
        default:
            throw `Unknown selector type ${selector}`;
        }
    }

    //Example def { "points": 20, "field": "level", "measurement": "hydro", "start": "-60h", "vals": "skip", "rows": ["1","-1"] }
    async verifyBucketContainsByDef(bucket, user, def){

        let define = JSON.parse(def);


        let query = `from(bucket: "${bucket}")
    |> range(start: ${define.start})
    |> filter(fn: (r) => r._measurement == "${define.measurement}")
    |> filter(fn: (r) => r._field == "${define.field}")`;

        let results = await influxUtils.query(user.orgid, query);


        let resTable = await (await results.split('\r\n')).filter((row) => {
            return row.split(',').length > 9;
        });

        let headers = resTable[0].split(',');
        headers.shift();
        let mesIn = headers.indexOf('_measurement');
        let fieldIn = headers.indexOf('_field');
        let valIn = headers.indexOf('_value');

        for(let i = 0; i < define.rows.length; i++){
            let recs;
            if(parseInt(define.rows[i]) === -1){  // last record
                recs = resTable[resTable.length - 1].split(',');
            }else{ //by normal index {0,1,2...N}
                recs = resTable[parseInt(define.rows[i])].split(',');
            }

            recs.shift();

            expect(recs[fieldIn]).to.equal(define.field);
            expect(recs[mesIn]).to.equal(define.measurement);

            if(typeof(define.vals) !== 'string'){
                expect(recs[valIn]).to.equal(define.vals[i]);
            }

        }
    }

    async clickPopupWizardContinue(){
        await this.basePage.getPopupWizardContinue().then(async button => {
            await button.click().then(async () => {
                // await this.driver.wait( // this wait is not consistent
                // await this.basePage.getUntilElementNotPresent(
                //basePage.getPopupWizardContinueSlector())) //
                await this.driver.sleep(500); // todo better wait
            });
        });
    }

    async clickPopupWizardPrevious(){
        await this.clickAndWait(await this.basePage.getPopupWizardBack()); // todo better wait
    }

    async clickPopupWizardFinish(){
        await this.clickAndWait(await this.basePage.getPopupWizardContinue()); //todo better wait
    }

    async dismissPopup(){
        await this.basePage.getPopupDismiss().then(async button => {
            await button.click().then(async () => {
                await this.driver.sleep(1000); // todo better wait - sometimes can be slow to unload
            });
        });
    }

    async clickPopupCancelBtn(){
        await this.clickAndWait(await this.basePage.getPopupCancel()); // todo better wait
    }

    async clickPopupCancelBtnSimple(){
        await this.clickAndWait(await this.basePage.getPopupCancelSimple(), async () => {
            try {
                await this.driver.wait(until.stalenessOf(await this.basePage.getPopupOverlay()));
            }catch(err){
                //  console.log("DEBUG err " + JSON.stringify(err));
                if(err.name !== 'NoSuchElementError'){ // O.K. if not found - DOM already updated
                    throw err;
                }
            }
        }); //todo better wait - try until overlay disappear
    }

    //sometimes need to lose focus from a popup element to trigger change
    async clickPopupTitle(){
        await this.clickAndWait(await this.basePage.getPopupTitle());
    }

    async verifyPopupNotPresent(){
        await this.assertNotPresent(await basePage.getPopupOverlayContainerSelector());
    }


    /*
      Since not currently refining waits - and focusing on velocity of adding new tests - use this
      for simple situations
     */
    async clickAndWait(element,
        wait = async () => { await this.driver.sleep((await this.driver.manage().getTimeouts()).implicit/20); }){ //wait 1/10th implicit timeout
        //console.log("DEBUG timeout " + ((await this.driver.manage().getTimeouts()).implicit/20));
        await element.click().then(async () => {
            await wait();
        });
    }

    async clickRAndWait(element,
                        wait = async () => { await this.driver.sleep((await this.driver.manage().getTimeouts()).implicit/20); }) { //wait 1/10th implicit timeout
        //console.log("DEBUG timeout " + ((await this.driver.manage().getTimeouts()).implicit/20));
        let action = this.driver.actions();
        await action.contextClick(element).perform().then(async () => {
           await wait();
        });
        //await element.click().then(async () => {
        //    await wait();
        //});
    }

    async typeTextAndWait(input, text,
        wait = async () => { await this.driver.sleep((await this.driver.manage().getTimeouts()).implicit/20); }) { //wait 1/10th implicit timeout)
        await input.sendKeys(text).then(async() => {
            await wait();
        });
    }

    async verifyElementText(element, text){
        await element.getText().then(async elText => {
            await expect(elText).to.equal(text);
        });
    }

    async verifyElementContainsText(element, text){
        await element.getText().then(async elText => {
            await expect(elText).to.include(text);
        });
    }

    async verifyElementContainsClass(element, clazz){
        await element.getAttribute('class').then(async elClass => {
            await expect(elClass).to.include(clazz);
        });
    }

    async verifyElementDoesNotContainClass(element, clazz){
        await element.getAttribute('class').then(async elClass => {
            await expect(elClass).to.not.include(clazz);
        });
    }

    async verifyElementDisabled(element){
        await element.getAttribute('disabled').then(async elAttr => {
            await expect(elAttr).to.not.be.null;
        });
    }

    async verifyElementEnabled(element){
        await element.getAttribute('disabled').then(async elAttr => {
            await expect(elAttr).to.be.null;
        });
    }

    async verifyWizardContinueButtonDisabled(){
        await this.verifyElementDisabled(await this.basePage.getPopupWizardContinue());
    }

    async verifyWizardDocsLinkURL(url){
        await this.basePage.getPopupWizardDocsLink().then(async elem => {
            await elem.getAttribute('href').then(async href => {
                await expect(href).to.equal(url);
            });
        });
    }

    async clearInputText(input){
        await input.sendKeys(Key.END);
        while((await input.getAttribute('value')).length > 0){
            await input.sendKeys(Key.BACK_SPACE);
        }
        await this.driver.sleep(200);
    }

    async verifyPopupAlertContainsText(text){
        await this.basePage.getPopupAlert().then(async elem => {
            await elem.findElement(By.css( '[class*=contents]')).then(async contentEl => {
                await contentEl.getText().then(async elText => {
                    await expect(elText).to.include(text);
                });
            });
        });
    }

    async verifyPopupAlertMatchesRegex(regex){
        await this.basePage.getPopupAlert().then(async elem => {
            await elem.findElement(By.css( '[class*=contents]')).then(async contentEl => {
                await contentEl.getText().then(async elText => {
                    await expect(elText).to.match(regex);
                });
            });
        });
    }

    async verifyInputEqualsValue(input, value){
        await input.getAttribute('value').then( async elVal => {
            await expect(elVal).to.equal(value);
        });
    }

    async verifyInputContainsValue(input, value){
        await input.getAttribute('value').then( async elVal => {
            await expect(elVal).to.include(value);
        });
    }

    async verifyInputDoesNotContainValue(input, value){
        await input.getAttribute('value').then( async elVal => {
            await expect(elVal).to.not.equal(value);
        });
    }

    //cmElem will be element containing class .CodeMirror
    async setCodeMirrorText(cmElem, text){
        //need to escape new lines which break the js code
        text = text.replace(/\n/g, '\\n');
        await this.driver.executeScript(`arguments[0].CodeMirror.setValue("${text}");`, cmElem);
        await this.driver.sleep(1000); //todo better wait - troubleshoot flakey consequences of this step
    }

    //get text
    //window.editor.getModel().getValueInRange(window.editor.getSelection())
    //
    //

    async setMonacoEditorText(monElem, text){
        //need to escape new lines which break the js code
        //text = text.replace(/\n/g, '\\n');
        //await this.driver.executeScript(`arguments[0].getModel().applyEdits([{ range monaco.Range.fromPositions(arguments[0].getPosition()), text: "${text}"}]);`, monElem);
        //await monElem.sendKeys(text);
        //let elem = await this.driver.findElement(By.css('.inputarea'));
        await monElem.click();
        await monElem.clear();
        await monElem.sendKeys(text);
        //await this.driver.executeScript(`arguments[0].setValue('HELLO')`, monElem);
        let leadParas = (text.match(/\(/g) || []).length;
        for(let i = 0; i < leadParas; i++){
            await monElem.sendKeys(Key.DELETE); //clean up autocomplete close paras TODO - fixme - find better solution
        }

        await this.driver.sleep(1000); //todo better wait - troubleshoot flakey consequences of this step
    }

    async clearMonacoEditorText(monElem){
        let elem = this.driver.findElement(By.css('.lines-content'));
        let text = await elem.getText();
        //await console.log("DEBUG text from monacoElement #" + text + "#");
        await monElem.sendKeys(Key.chord(Key.CONTROL, Key.END));
        for( let i = 0; i < text.length; i++){
            await monElem.sendKeys(Key.BACK_SPACE);
        }
    }

    async sendMonacoEditorKeys(monElem, keys){
        let actionList = keys.split(',');
        for(let i = 0; i < actionList.length; i++){
            if(actionList[i].includes('+')){ //is chord
                let chord = actionList[i].split('+');
                if(chord.length === 2){
                    await monElem.sendKeys(Key.chord(keyMap[chord[0].toLowerCase()],
                        keyMap[chord[1].toLowerCase()]));
                }else if(chord.length === 3){
                    await monElem.sendKeys(Key.chord(keyMap[chord[0].toLowerCase()],
                        keyMap[chord[1].toLowerCase()],
                        keyMap[chord[2].toLowerCase()]));

                }else{
                    throw `unsupported chord count ${actionList[i]}`;
                }

            }else{
                await monElem.sendKeys(keyMap[actionList[i].toLowerCase()]);
            }
        }
    }

    async getMonacoEditorText(){
        return await this.driver.executeScript('return this.monaco.editor.getModels()[0].getValue()');
    }

    async getCodeMirrorText(cmElem){
        return await this.driver.executeScript('return arguments[0].CodeMirror.getValue()', cmElem);
    }

    async verifyFormErrorMessageContains(msg){
        await this.verifyElementContainsText(await this.basePage.getPopupFormElementMessage(), msg);
    }

    async verifySubmitDisabled(){
        await this.verifyElementDisabled(await this.basePage.getPopupSubmit());
    }

    async clickPopupSubmitButton(){
        await this.clickAndWait(await this.basePage.getPopupSubmit());
    }

    async verifyElementContainsAttribute(elem, attrib){
        await elem.getAttribute(attrib).then(async val => {
            await expect(val).to.exist;
        });
    }

    async verifyElementAttributeContainsText(elem,attrib,text){
        await elem.getAttribute(attrib).then(async at => {
            await expect(at).to.include(text);
        });
    }

    async verifyElementContainsNoText(elem){
        await elem.getText().then(async text => {
            await expect(text.length).to.equal(0);
        });
    }

    async setFileUpload(filePath){
        await this.basePage.getPopupFileUpload().then(async elem => {
            await elem.sendKeys(process.cwd() + '/' + filePath).then(async () => {
                await this.delay(200); //debug wait - todo better wait
            });
        });
    }

    async verifyPopupWizardStepStateText(text){
        await this.verifyElementContainsText(await this.basePage.getPopupWizardStepStateText(), text);
    }

    async verifyPopupWizardStepState(state){
        await this.verifyElementContainsClass(await this.basePage.getPopupWizardStepStateText(), state);
    }

    async verifyPopupFileUploadHeaderText(text){
        await this.verifyElementContainsText(await this.basePage.getPopupFileUploadHeader(), text);
    }

    async clickPageTitle(){
        await this.clickAndWait(await this.basePage.getPageTitle());
    }

    async copyFileContentsToTextarea(filepath, textarea){
        let buffer = await influxUtils.readFileToBuffer(process.cwd() + '/' + filepath);
        await textarea.sendKeys(buffer);
    }

    async pressKeyAndWait(key, wait = async () => { await this.driver.sleep((await this.driver.manage().getTimeouts()).implicit/20); }){
        await this.driver.switchTo().activeElement().then(async elem => {
            await elem.sendKeys(keyMap[key.toLowerCase()]).then(async () => {
                await wait();
            });
        });
    }

    async verifyFileExists(filePath){
        if(__config.sel_docker){
            console.warn('File export not supported without shared memory');
            return;
        }
        await expect(await influxUtils.fileExists(filePath)).to.be.true;
    }

    async verifyFileMatchingRegexExists(regex){
        let res = await influxUtils.verifyFileMatchingRegexFilesExist(regex);
        await expect(res).to.be.true;
    }

    async verifyFirstCSVFileMatching(path, dataDescr){
        let deck = [0,1,2,3,4,5,6,7,8,9];
        //shuffle deck
        for(let i = 0; i < deck.length; i++){
            let target = Math.floor(Math.random() * Math.floor(deck.length - 1));
            //swap
            let temp = deck[i];
            deck[i] = deck[target];
            deck[target] = temp;
        }

        let file = await influxUtils.getNthFileFromRegex(path, 1);
        let content = await influxUtils.readFileToBuffer(file);
        let firstLF = content.indexOf('\n');
        let firstLine = content.slice(0,firstLF);

        assert.match(firstLine.trim(),/^#group,.*$/);

        let csvContent = await influxUtils.readCSV(content);

        for(let i = 0; i < 3; i++) {
            Object.keys(dataDescr).forEach(async (key) => {

                if (dataDescr[key].includes("type:")) { //then just test type
                    let testType = dataDescr[key].split(':')[1];
                    switch (testType) {
                        case 'double':
                        case 'float':
                        case 'number':
                            await assert.match(csvContent[deck[i]][key], /^[-+]?\d*\.?\d*$/, `${key} should be of type double/float/number`);
                            break;
                        case 'date':
                            //2020-02-27T13:51:09.7Z
                            await assert.match(csvContent[deck[i]][key], /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.?\d{0,3}Z$/,
                                `${key} should be of type date`);
                            break;
                        default:
                            throw `unhandled test type ${testType}`;
                    }
                } else {
                    expect(dataDescr[key].trim()).to.equal(csvContent[deck[i]][key].trim());
                }
            })
        }
    }

    async scrollElementIntoView(elem){
        await this.assertVisible(elem).catch(async () => {
            await this.driver.executeScript('arguments[0].scrollIntoView(true);', elem).then(async () => {
                await this.driver.sleep(150);
            });
        });
    }

    async writeBase64ToPNG(filePath, base64String){
        let base64Data = base64String.replace(/^data:image\/png;base64,/,'');
        fs.writeFile(filePath,
            base64Data,
            'base64',
            async err => {
                if(err) {
                    console.log(err);
                }
            });
    }

    async writeMessageToConsoleLog(msg){
        await this.driver.executeScript('console.info(\'TO WHOM IT MAY CONCERN\')');
        await this.driver.executeScript('console.log(\'HELLO\')');
        await this.driver.executeScript('console.warn(\'BOO BOO BOO!\')');
        await this.driver.executeScript('console.error(\'AIEYAIYAI!\')');
        //try {
            await this.driver.executeScript('throw new Error(\'WHADDAP?\')');
        //}catch(e){
           //ignore
        //}
    }

    async getConsoleLog(){
        console.log("DEBUG logging.Type.BROWSER " + logging.Type.BROWSER);
        await this.driver.manage().logs().get(logging.Type.BROWSER).then(async logs => {
            await console.log("DEBUG typeof(logs) " + JSON.stringify(logs));
            for(let log in logs){
                await console.log("DEBUG logs[log] #" + logs[log].message + "#");
            }
        })
    }

    async sendKeysToCurrent(keys){
        let current = await this.driver.switchTo().activeElement();
        let actionList = keys.split(',');
        for(let i = 0; i < actionList.length; i++){
            if(actionList[i].includes('+')){ //is chord
                let chord = actionList[i].split('+');
                if(chord.length === 2){
                    await current.sendKeys(Key.chord(keyMap[chord[0].toLowerCase()],
                        keyMap[chord[1].toLowerCase()]));
                }else if(chord.length === 3){
                    await current.sendKeys(Key.chord(keyMap[chord[0].toLowerCase()],
                        keyMap[chord[1].toLowerCase()],
                        keyMap[chord[2].toLowerCase()]));

                }else{
                    throw `unsupported chord count ${actionList[i]}`;
                }

            }else{
                await current.sendKeys(keyMap[actionList[i].toLowerCase()]);
            }
        }
    }

    async startLiveDataGenerator(def){
        await influxUtils.startLiveDataGen(def);
    }

    async stopLiveDataGenerator() {
        await influxUtils.stopLiveDataGen();
    }

    async clickSortTypeDropdown(){
        await this.clickAndWait(await this.basePage.getSortTypeButton());
    }

    async clickSortByListItem(item){
        //Check for special items
        if(item.startsWith('retentionRules[0]')){
            //don't normalize the string
            await this.clickAndWait(await this.basePage.getSortTypeListItem(item, false));
        }else{
            await this.clickAndWait(await this.basePage.getSortTypeListItem(item));
        }
    }

    async verifyAddLabelPopoverVisible(){
        await this.assertVisible(await this.basePage.getLabelPopover());
    }

    async verifyAddLabelsPopopverNotPresent(){
        await this.assertNotPresent(await basePage.getLabelPopoverSelector());
    }

    async verifyLabelPopoverContainsLabels(labels){
        let labelsList = labels.split(',');
        for(const label of labelsList){
            let elem = await this.basePage.getLabelListItem(label.trim())
            await this.scrollElementIntoView(elem);
            await this.assertVisible(elem);
        }
    }

    async verifyLabelPopoverDoesNotContainLabels(labels){
        let labelsList = labels.split(',');
        for(const label of labelsList){
            await this.assertNotPresent(await basePage.getLabelListItemSelector(label.trim()));
        }

    }

    async clickLabelPopoverItem(item){
        await this.clickAndWait(await this.basePage.getLabelListItem(item.trim()));
    }

    async setLabelPopoverFilter(val){
        await this.clearInputText(await this.basePage.getLabelPopoverFilterField());
        await this.typeTextAndWait(await this.basePage.getLabelPopoverFilterField(), val);
    }

    async verifyLabelPopoverCreateNew(name){
        await this.verifyElementContainsText(await this.basePage.getLabelPopoverCreateNewButton(), name);
    }

    async verifyLabelPopupNoCreateNew(){
        await this.assertNotPresent(await basePage.getLabelPopoverCreateNewButtonSelector());
    }


    async clearDashboardLabelsFilter(){
        await this.clearInputText(await this.basePage.getLabelPopoverFilterField());
    }



}

module.exports = baseSteps;
