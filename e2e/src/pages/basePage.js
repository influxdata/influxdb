const { By, Condition, until, StaleElementReferenceError} = require('selenium-webdriver');

const notificationSuccessMsg = '[data-testid=notification-success] .notification--message';
const notificationErrorMsg = '[data-testid=notification-error] .notification--message';
const notificationPrimaryMsg = '[data-testid=notification-primary] .notification--message';
const notificationCloseButton = '[data-testid^=notification-] button';
const popupOverlayContainer = '[data-testid=overlay--container]';
const popupFormElementError  = '[data-testid=form--element-error]';
const formInputError = '[data-testid=input-error]';
const popupOverlay = '[data-testid=overlay]';
const popupDismiss = '[data-testid=overlay--header] button[class*=dismiss]';
const popupCancel = '[data-testid=overlay] button[data-testid=button--cancel]';
const popupCancelSimple = '[data-testid=overlay--body] button[title=Cancel] ';
const popupWizardContinue = '[data-testid=overlay--body] [data-testid=next]';
const popupSave = '[data-testid=overlay--container] button[data-testid=button--save] ';
const popupSaveSimple = '[data-testid=overlay--footer] button[title=\'Save\']';
const popupCreate = '[data-testid=overlay--container] button[title=Create]';
const popupSubmit = '[data-testid=button][type=submit]';
const popupCopyToClipboard = '[data-testid=button-copy][title=\'Copy to Clipboard\']';
const popupWizardBack = '[data-testid=overlay--body] [data-testid=back]';
const popupWizardTitle = '[data-testid=overlay--body] .wizard-step--title';
const popupWizardSubTitle = '[data-testid=overlay--body] .wizard-step--sub-title';
const popupWizardDocsLink = '[data-testid=overlay--body] [data-testid=docs-link]';
const popupWizardStepStateText = 'p.line-protocol--status:nth-of-type(1)';
const popupTitle = '[data-testid=overlay--header] .cf-overlay--title';
const codeMirror = 'div.CodeMirror';
const monacoEditor = '.monaco-editor';
const popupAlert = '[data-testid=alert]';
const popupFileUpload = 'input[type=file]';
const popupFileUploadHeader = '.drag-and-drop--header';
const pageTitle = '[data-testid=page-title] ';
const popupBody = '[data-testid=overlay--body]';
const popupGithubLink = '//a[contains(text(), \'GitHub Repository\')]';
const popoverDialog = '[data-testid=popover--dialog]';

//generic controls
const sortTypeButton = '[data-testid=resource-sorter--button]';
const sortTypeListItem = '[data-testid=\'resource-sorter--%ITEM%\']';
const dropdownContents = '[data-testid=dropdown-menu--contents]';
const dropdownItemByText = '//*[@data-testid=\'dropdown-item\'][./*[text()=\'%TEXT%\']]';

//common controls
const labelPopover = '[data-testid=\'inline-labels--popover--dialog\']';
const labelListItem = '[data-testid=\'label-list--item %ITEM%\']';
const labelPopoverFilterField = '[data-testid=\'inline-labels--popover-field\']';
const labelPopoverCreateNewButton = '[data-testid=\'inline-labels--create-new\']';


class basePage{

    constructor(driver){
        this.driver = driver;
    }

    delay(timeout){
        return new Promise((resolve) => {
            setTimeout(resolve, timeout);
        });
    }

    async waitUntilElementCss(selector){
        await this.driver.wait(until.elementLocated(By.css(selector)));
    }

    async waitUntilElementVisibleCss(selector){
        await this.driver.wait(until.elementIsVisible(this.driver.findElement(By.css(selector))));
    }


    // selectors should be array of {type, selector}
    async isLoaded(selectors, url = undefined){
        if(url){
            await this.driver.wait(until.urlContains(url), 5000);
        }

        //selectors.forEach(async (selector) => { //N.B. for each seems to be swallowing thrown errors
        for(let i = 0; i < selectors.length; i++) {
            switch (selectors[i].type) {
            case 'css':
                await this.driver.wait(until.elementLocated(By.css(selectors[i].selector)), 5000);
                break;
            case 'xpath':
                await this.driver.wait(until.elementLocated(By.xpath(selectors[i].selector)), 5000);
                break;
            default:
                throw `Unkown selector type ${JSON.stringify(selectors[i])}`;
            }

            // TODO - implement other selector types
        }
        //});
    }

    async getPopupOverlay(){
        return await this.driver.findElement(By.css(popupOverlay));
    }

    static getPopupOverlaySelector(){
        return { type: 'css', selector: popupOverlay};
    }

    async getNoficicationSuccessMsgs(){
        await this.waitUntilElementVisibleCss(notificationSuccessMsg);
        return await this.driver.findElements(By.css(notificationSuccessMsg));
    }

    async getNotificationErrorMsgs(){
        await this.waitUntilElementVisibleCss(notificationErrorMsg);
        return await this.driver.findElements(By.css(notificationErrorMsg));
    }

    async getNotificationPrimaryMsgs(){
        await this.waitUntilElementVisibleCss(notificationPrimaryMsg);
        return await this.driver.findElements(By.css(notificationPrimaryMsg));

    }

    async getNotificationCloseButtons(){
        return await this.driver.findElements(By.css(notificationCloseButton));
    }

    // selector shold be of {type, selector}
    // helper to avoid stale element exceptions etc.
    async smartGetElement(selector, timeout = this.driver.manage().getTimeouts().implicit) {
        let resultElem; // check staleness with resultElem.enabled() or similar
        for (let i = 0; i < 3; i++) {
            try {
                switch (selector.type) {
                case 'css':
                    await this.driver.wait(until.elementLocated(By.css(selector.selector)), timeout);
                    resultElem = await this.driver.findElement(By.css(selector.selector)).catch(async err => {
                        console.log('DEBUG CAUGHT ERROR ' + JSON.stringify(err));
                        console.log('AT ' + selector.selector);
                        throw err;
                    });
                    break;
                case 'xpath':
                    await this.driver.wait(until.elementLocated(By.xpath(selector.selector)), timeout);
                    resultElem = await this.driver.findElement(By.xpath(selector.selector)).catch(async err => {
                        console.log('DEBUG CAUGHT ERROR ' + JSON.stringify(err));
                        console.log('AT ' + selector.selector);
                        throw err;
                    });
                    break;
                default:
                    throw `Unkown selector type ${JSON.stringify(selector)}`;
                }
                await resultElem.isEnabled();
            } catch (e) {
                if (e instanceof StaleElementReferenceError && i !== 2) {
                    console.log('DEBUG caught ' + e);
                    //continue - try to get elem again
                } else {
                    throw e;
                }
            }
        }
        return resultElem;
    }

    // selector should be of {type, selector}
    async getUntilElementNotPresent(selector){
        return new Condition('for no element to be located ' + selector, async () => {
            switch(selector.type) {
            case 'css':
                return await this.driver.findElements(By.css(selector.selector)).then(function (elements) {
                    return elements.length === 0;
                });
            case 'xpath:':
                return await this.driver.findElements(selector).then(function (elements) {
                    return elements.length === 0;
                });
            default:
                throw `Unkown selector type ${JSON.stringify(selector)}`;
            }
        });
    }

    async getPopupOverlayContainer(){
        return await this.driver.findElement(By.css(popupOverlayContainer));
    }

    static getPopupOverlayContainerSelector(){
        return { type: 'css', selector: popupOverlayContainer};
    }

    async getPopupFormElementError(){
        return await this.driver.findElement(By.css(popupFormElementError));
    }

    static getPopupFormElementErrorSelector(){
        return { type: 'css', selector: popupFormElementError};
    }

    async getFormInputErrors(){
        return await this.driver.findElements(By.css(formInputError));
    }

    static getFormInputErrorSelector(){
        return { type: 'css', selector: formInputError};
    }

    async getPopupDismiss(){
        return await this.driver.findElement(By.css(popupDismiss));
    }

    async getPopupCancel(){
        return await this.driver.findElement(By.css(popupCancel));
    }

    async getPopupCancelSimple(){
        return await this.driver.findElement(By.css(popupCancelSimple));
    }

    async getPopupSave(){
        return await this.driver.findElement(By.css(popupSave));
    }

    async getPopupSaveSimple(){
        return await this.driver.findElement(By.css(popupSaveSimple));
    }

    async getPopupSubmit(){
        return await this.driver.findElement(By.css(popupSubmit));
    }

    async getPopupWizardContinue(){
        return await this.driver.findElement(By.css(popupWizardContinue));
    }

    static getPopupWizardContinueSelector(){
        return {type: 'css', selector: popupWizardContinue};
    }

    async getPopupWizardBack(){
        return await this.driver.findElement(By.css(popupWizardBack));
    }

    static getPopupWizardBackSelector(){
        return {type: 'css', selector: popupWizardBack};
    }

    async getPopupWizardTitle(){
        return await this.driver.findElement(By.css(popupWizardTitle));
    }

    static getPopupWizardTitleSelector(){
        return {type: 'css', selector: popupWizardTitle};
    }

    async getPopupWizardSubTitle(){
        return await this.driver.findElement(By.css(popupWizardSubTitle));
    }

    static getPopupWizardSubTitleSelector(){
        return {type: 'css', selector: popupWizardSubTitle};
    }

    async getPopupWizardDocsLink(){
        return await this.driver.findElement(By.css(popupWizardDocsLink));
    }

    async getPopupWizardStepStateText(){
        return await this.driver.findElement(By.css(popupWizardStepStateText));
    }

    async getPopupTitle(){
        return await this.driver.findElement(By.css(popupTitle));
    }

    async getCodeMirror(){
        return await this.driver.findElement(By.css(codeMirror));
    }

    async getMonacoEditor(){
        return await this.driver.findElement(By.css(monacoEditor));
    }

    async getPopupAlert(){
        return await this.driver.findElement(By.css(popupAlert));
    }

    async getPopupCopyToClipboard(){
        return await this.driver.findElement(By.css(popupCopyToClipboard));
    }

    async getPopupCreate(){
        return await this.driver.findElement(By.css(popupCreate));
    }


    async getPopupFormElementMessage(){
        return await this.driver.findElement(By.css(popupFormElementError));
    }

    async getPopupFileUpload(){
        return await this.driver.findElement(By.css(popupFileUpload));
    }

    async getPopupFileUploadSelector(){
        return {type: 'css', selector: popupFileUpload};
    }

    async getPopupFileUploadHeader(){
        return await this.driver.findElement(By.css(popupFileUploadHeader));
    }

    async getPageTitle(){
        return await this.driver.findElement(By.css(pageTitle));
    }

    async getPopupBody(){
        return await this.driver.findElement(By.css(popupBody));
    }

    static getPopupBodySelector(){
        return { type: 'css', selector: popupBody };
    }

    async getPopupGithubLink(){
        return await this.driver.findElement(By.xpath(popupGithubLink));
    }

    async getSortTypeButton(){
        return await this.driver.findElement(By.css(sortTypeButton));
    }

    static getSortTypeButtonSelector(){
        return { type: 'css', selector: sortTypeButton }
    }

    async getSortTypeListItem(item, normalize = true){
        if(normalize) {
            return await this.driver.findElement(By.css(sortTypeListItem.replace('%ITEM%', item.toLowerCase()
                .replace(" ", "-"))));
        }else{
            return await this.driver.findElement(By.css(sortTypeListItem.replace('%ITEM%', item)));
        }
    }

    async getDropdownContents(){
        return await this.driver.findElement(By.css(dropdownContents));
    }

    static getDropdownContentsSelector(){
        return { type: 'css', selector: dropdownContents };
    }

    async getDropdownItemByText(text){
        return await this.driver.findElement(By.xpath(dropdownItemByText.replace('%TEXT%', text.trim())));
    }

    async getPopoverDialog(){
        return await this.driver.findElement(By.css(popoverDialog));
    }

    static getpopoverDialogSelector(){
        return { type: 'css', selector: popoverDialog };
    }

    async getLabelPopover(){
        return await this.driver.findElement(By.css(labelPopover));
    }

    static getLabelPopoverSelector(){
        return { type: 'css', selector: labelPopover };
    }

    async getLabelListItem(item){
        return await this.driver.findElement(By.css(labelListItem.replace('%ITEM%', item)));
    }

    static getLabelListItemSelector(item){
        return { type: 'css', selector: labelListItem.replace('%ITEM%', item) };
    }

    async getLabelPopoverFilterField(){
        return await this.driver.findElement(By.css(labelPopoverFilterField));
    }

    async getLabelPopoverCreateNewButton(){
        return await this.driver.findElement(By.css(labelPopoverCreateNewButton));
    }

    static getLabelPopoverCreateNewButtonSelector(){
        return { type: 'css', selector: labelPopoverCreateNewButton };
    }




}

module.exports = basePage;
