const influxPage = require(__srcdir + '/pages/influxPage.js');
const { By } = require('selenium-webdriver');

const pageTitle = '[data-testid=\'page-title\']';
const createCheckButton = '[data-testid=create-check]';
const createEndpointButton = '[data-testid=create-endpoint]';
const createRuleButton = '[data-testid=create-rule]';
const checksFilterInput = '[data-testid=\'filter--input checks\']';
const checksQuestionMark = '[data-testid=\'Checks--question-mark\']';
const checksTooltipContents = '[data-testid=\'Checks--question-mark-tooltip--contents\']';
const alertingTab = '[data-testid=alerting-tab--%TABNAME%]';
const createCheckDropdown = '[data-testid=\'checks--column\'] [data-testid=\'dropdown-menu--contents\']';
const createCheckDropdownItem = '[data-testid=\'dropdown-menu--contents\'] [data-testid=create-%ITEM%-check]';
const endpointsFilterInput = '[data-testid=\'filter--input endpoints\']';
const endpointsQuestionMark = '[data-testid=\'Notification Endpoints--question-mark\']';
const endpointsTooltipContents = '[data-testid=\'Notification Endpoints--question-mark-tooltip--contents\']';
const rulesFilterInput = '[data-testid=\'filter--input rules\']';
const rulesQuestionMark = '[data-testid=\'Notification Rules--question-mark\']';
const rulesTooltipContents = '[data-testid=\'Notification Rules--question-mark-tooltip--contents\']';
const firstTimeThresholdCheckCreateButton = '[data-testid=\'checks--column\'] [data-testid=panel--body] [data-testid=button][title=\'Threshold Check\']';
const firstTimeDeadmanCheckCreateButton = '[data-testid=\'checks--column\'] [data-testid=panel--body] [data-testid=button][title=\'Deadman Check\']';
const emptyStateColumnText = '[data-testid=\'%COL%--column\'] [data-testid=\'empty-state--text\']';

//Resource card
const checkCardName = '//*[@data-testid=\'check-card--name\'][./*[text()=\'%NAME%\']]';
const checkCardNameEditButton = '//*[./*/*[text()=\'%NAME%\']]//*[@data-testid=\'check-card--name-button\']';
const checkCardNameInput = '[data-testid=check-card--input]';
const checkCardDescription = '//*[@data-testid=\'check-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'resource-list--editable-description\']';
const checkCardDescriptionEditButton = '//*[@data-testid=\'check-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'resource-list--editable-description\']//*[@data-testid=\'icon\']';
const checkCardDescriptionInput = '[data-testid=check-card] [data-testid=input-field]';
const checkCardAddLabelButton = '//*[@data-testid=\'check-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'inline-labels--add\']';
const checkCardLabelEmpty = '//*[@data-testid=\'check-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'inline-labels--empty\']';
const checkCardLabelPill = '//*[@data-testid=\'check-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'label--pill %LABEL%\']';
const checkCardLabelRemove = '//*[@data-testid=\'check-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'label--pill--delete %LABEL%\']';
const checkCardCloneButton = '//*[@data-testid=\'check-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'context-menu\'][./*[contains(@class,\'duplicate\')]]';
const checkCardCloneConfirm = '//*[@data-testid=\'check-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'context-menu-item\'][text()=\'Clone\']';
const checkCardOpenHistory = '//*[@data-testid=\'check-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'context-menu\'][./*[contains(@class,\'eye-open\')]]';
const checkCardOpenHistoryConfirm = '//*[@data-testid=\'check-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'context-menu-item\']';
const checkCardDeleteButton = '//*[@data-testid=\'check-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'context-delete-menu\']';
const checkCardDeleteConfirm = '//*[@data-testid=\'check-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'context-delete-task\']'


//Create Endpoint Popup
const epPopupEndpointDropdownButton = '[data-testid=endpoint--dropdown--button]';
const epPopupEndpointNameInput = '[data-testid=endpoint-name--input]';
const epPopupEndpointDescriptionText = '[data-testid=endpoint-description--textarea]';
const epPopupCancelButton = '[data-testid=endpoint-cancel--button]';
const epPopupSaveButton = '[data-testid=endpoint-save--button]';

//Query Builder
const qbSelectorListItem = '[data-testid=\'selector-list %ITEM%\']';


const urlCtx = 'alerting';

class alertsPage extends influxPage {

    constructor(driver){
        super(driver);
    }

    async isLoaded(){
        await super.isLoaded([{type: 'css', selector: pageTitle},
            {type: 'css', selector: createCheckButton},
            {type: 'css', selector: createEndpointButton},
            {type: 'css', selector: createRuleButton},
            {type: 'css', selector: checksFilterInput},
            {type: 'css', selector: endpointsFilterInput},
            {type: 'css', selector: rulesFilterInput},
        ], urlCtx);
    }


    async getPageTitle(){
        return await this.driver.findElement(By.css(pageTitle));
    }

    async getAlertingTab(tabName){
        return await this.driver.findElement(By.css(alertingTab.replace('%TABNAME%', tabName)));
    }

    async getCreateCheckButton(){
        return await this.driver.findElement(By.css(createCheckButton));
    }

    async getCreateEndpointButton(){
        return await this.driver.findElement(By.css(createEndpointButton));
    }

    async getCreateRuleButton(){
        return await this.driver.findElement(By.css(createRuleButton));
    }

    async getChecksQuestionMark(){
        return await this.driver.findElement(By.css(checksQuestionMark));
    }

    async getChecksFilterInput(){
        return await this.driver.findElement(By.css(checksFilterInput));
    }

    async getChecksTooltipContents(){
        return await this.driver.findElement(By.css(checksTooltipContents));
    }

    static getChecksTooltipContentsSelector(){
        return { type: 'css', selector: checksTooltipContents };
    }

    async getEndpointsQuestionMark(){
        return await this.driver.findElement(By.css(endpointsQuestionMark));
    }

    async getEndpointsFilterInput(){
        return await this.driver.findElement(By.css(endpointsFilterInput));
    }

    async getEndpointsTooltipContents(){
        return await this.driver.findElement(By.css(endpointsTooltipContents));
    }

    static getEndpointsTooltipContentsSelector(){
        return { type: 'css', selector: endpointsTooltipContents };
    }

    async getRulesFilterInput(){
        return await this.driver.findElement(By.css(rulesFilterInput));
    }

    async getRulesQuestionMark(){
        return await this.driver.findElement(By.css(rulesQuestionMark));
    }

    async getRulesTooltipContents(){
        return await this.driver.findElement(By.css(rulesTooltipContents));
    }

    static getRulesTooltipContentsSelector(){
        return { type: 'css', selector: rulesTooltipContents };
    }

    async getFirstTimeThresholdCheckCreateButton(){
        return await this.driver.findElement(By.css(firstTimeThresholdCheckCreateButton));
    }

    async getFirstTimeDeadmanCheckCreateButton(){
        return await this.driver.findElement(By.css(firstTimeDeadmanCheckCreateButton));
    }

    async getCreateCheckDropdownItem(item){
        return await this.driver.findElement(By.css(createCheckDropdownItem.replace('%ITEM%', item.toLowerCase())));
    }

    static getCreateCheckDropdownSelector(){
        return { type: 'css', selector: createCheckDropdown }
    }

    async getEpPopupEndpointDropdownButton(){
        return await this.driver.findElement(By.css(epPopupEndpointDropdownButton));
    }

    async getEpPopupEndpointNameInput(){
        return await this.driver.findElement(By.css(epPopupEndpointNameInput));
    }

    async getEpPopupEndpointDescriptionText(){
        return await this.driver.findElement(By.css(epPopupEndpointDescriptionText));
    }

    async getEpPopupCancelButton(){
        return await this.driver.findElement(By.css(epPopupCancelButton));
    }

    async getEpPopupSaveButton(){
        return await this.driver.findElement(By.css(epPopupSaveButton));
    }

    async getQbSelectorListItem(item){
        return await this.driver.findElement(By.css(qbSelectorListItem.replace('%ITEM%', item)))
    }

    async getCheckCardName(name){
        return await this.driver.findElement(By.xpath(checkCardName.replace('%NAME%', name)));
    }

    static getCheckCardNameSelector(name){
        return {type: 'xpath', selector: checkCardName.replace('%NAME%', name)}
    }

    async getCheckCardNameEditButton(name){
        return await this.driver.findElement(By.xpath(checkCardNameEditButton.replace('%NAME%', name)));
    }

    async getCheckCardNameInput(){
        return await this.driver.findElement(By.css(checkCardNameInput));
    }

    async getCheckCardDescription(name){
        return await this.driver.findElement(By.xpath(checkCardDescription.replace('%NAME%', name)));
    }

    async getCheckCardDescriptionEditButton(name){
        return await this.driver.findElement(By.xpath(checkCardDescriptionEditButton.replace('%NAME%', name)));
    }

    async getCheckCardDescriptionInput(){
        return await this.driver.findElement(By.css(checkCardDescriptionInput));
    }

    async getCheckCardAddLabelButton(name){
        return await this.driver.findElement(By.xpath(checkCardAddLabelButton.replace('%NAME%', name)));
    }

    async getCheckCardLabelEmpty(name){
        return await this.driver.findElement(By.xpath(checkCardLabelEmpty.replace('%NAME%', name)));
    }

    async getCheckCardLabelPill(name, label){
        return await this.driver.findElement(By.xpath(checkCardLabelPill
            .replace('%NAME%', name)
            .replace('%LABEL%', label)));
    }

    static getCheckCardLabelPillSelector(name, label){
        return { type: 'xpath', selector: checkCardLabelPill
                .replace('%NAME%', name)
                .replace('%LABEL%', label)}
    }

    async getCheckCardLabelRemove(name, label){
        return await this.driver.findElement(By.xpath(checkCardLabelRemove
            .replace('%NAME%', name)
            .replace('%LABEL%', label)));
    }

    async getCheckCardCloneButton(name){
        return await this.driver.findElement(By.xpath(checkCardCloneButton.replace('%NAME%', name)))
    }

    async getCheckCardCloneConfirm(name){
        return await this.driver.findElement(By.xpath(checkCardCloneConfirm.replace('%NAME%', name)))
    }

    async getEmptyStateColumnText(col){
        return await this.driver.findElement(By.css(emptyStateColumnText.replace('%COL%', col)));
    }

    async getCheckCardOpenHistory(name){
        return await this.driver.findElement(By.xpath(checkCardOpenHistory.replace('%NAME%', name)))
    }

    async getCheckCardOpenHistoryConfirm(name){
        return await this.driver.findElement(By.xpath(checkCardOpenHistoryConfirm.replace('%NAME%', name)));
    }

    async getCheckCardDeleteButton(name){
        return await this.driver.findElement(By.xpath(checkCardDeleteButton.replace('%NAME%', name)));
    }

    async getCheckCardDeleteConfirm(name){
        return await this.driver.findElement(By.xpath(checkCardDeleteConfirm.replace('%NAME%', name)));
    }

}

module.exports = alertsPage;
