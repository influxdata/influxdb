const influxPage = require(__srcdir + '/pages/influxPage.js');
const { By } = require('selenium-webdriver');

const thresholds = ["CRIT", "WARN", "INFO", "OK"];

const overlay = '[data-testid=overlay]';
const dismissButton = '[data-testid=page-control-bar] [data-testid=square-button]';
const saveCellButton = '[data-testid=overlay] [data-testid=save-cell--button]';
const pageCheckEditTitle = '[data-testid=overlay] [data-testid=page-header] [data-testid=page-title]';
const pageCheckEditTitleInput = '[data-testid=\'page-header\'] [data-testid=\'input-field\']';
const queriesToggle = '[data-testid=overlay] [data-testid=select-group--option][title=queries]';
const configureCheckToggle = '[data-testid=overlay] [data-testid=\'checkeo--header alerting-tab\']';
const defineQueryToggle = '[data-testid=select-group--option]';
const checklistPopover = '[data-testid=popover--contents] [class=query-checklist--popover]';
const checklistPopoverItemByText = '//*[@data-testid=\'popover--contents\']//*[text()=\'%TEXT%\']';

//Preview area
const previewThresholdHandleByLevel = '[class*=\'threshold-marker--handle threshold-marker--%LEVEL%\']';


// TODO timemachine controls -- see dashboards - try and reuse

// Configure Check Controls

//    Properties
const confChkIntervalInput = '//*[./label/span[text() = \'Schedule Every\']]//*[@data-testid=\'duration-input\']';
const confChkOffset = '//*[./label/span[text() = \'Offset\']]//*[@data-testid=\'duration-input\']';
const confChkAddTagButton = '//*[./label/span[text() = \'Tags\']]//*[@data-testid=\'dashed-button\']';
//    Status Message Template
const confChkMessageTextArea = '[data-testid=status-message-textarea]';

const confTagRuleKeyInputOfTag = '[data-testid=tag-rule]:nth-of-type(%INDEX%) [data-testid=\'tag-rule-key--input\'][name=key]';
const confTagRuleValueInputOfTag = '[data-testid=tag-rule]:nth-of-type(%INDEX%) [data-testid=\'tag-rule-key--input\'][name=value]';
const confTagRuleDimissOfTag = '[data-testid=tag-rule]:nth-of-type(%INDEX%) [data-testid=dismiss-button]';


// Thresholds
const confChkAddThresholdButton = '[data-testid=add-threshold-condition-%STATUS%]';
const confNthThresholdDefDropdownButton = '[data-testid=overlay] [data-testid=panel]:nth-of-type(%INDEX%) [data-testid=select-option-dropdown]';
const confNthThresholdDefDropdownItem = '[data-testid=overlay] [data-testid=panel]:nth-of-type(%INDEX%) [data-testid=select-option-dropdown] [data-testid=dropdown-item][title=\'%ITEM%\']';
const confNthThresholdDefInput = '[data-testid=overlay] [data-testid=panel]:nth-of-type(%INDEX%) [data-testid=input-field]';
const confNthThreshold2ndInput = '[data-testid=overlay] [data-testid=panel]:nth-of-type(%INDEX%) [data-testid^=\'component-spacer--flex-child\']:nth-of-type(3) [data-testid=input-field]';
//Deadman
const confDeadmanForInput = '//*[@data-testid=\'component-spacer\']/*[@data-testid=\'component-spacer\'][.//*[text()=\'for\']]//*[@data-testid=\'duration-input\']';
const confDeadmanForInputDropdownItem = '//*[@data-testid=\'component-spacer\']/*[@data-testid=\'component-spacer\'][.//*[text()=\'for\']]//*[@data-testid=\'dropdown-item\'][.//*[text()=\'%ITEM%\']]';
const confDeadmanStopInput = '//*[@data-testid=\'component-spacer\']/*[@data-testid=\'component-spacer\'][.//*[text()=\'And stop checking after\']]//*[@data-testid=\'duration-input\']';
const confDeadmanStopInputDropdownItem = '//*[@data-testid=\'component-spacer\']/*[@data-testid=\'component-spacer\'][.//*[text()=\'And stop checking after\']]//*[@data-testid=\'dropdown-item\'][.//*[text()=\'%ITEM%\']]';
const confDeadmanCheckLevelsDropdown = '//*[@data-testid=\'component-spacer\']/*[@data-testid=\'component-spacer\']//*[@data-testid=\'check-levels--dropdown--button\']';
const confDeadmanCheckLevelsDropodownItem = '[data-testid=\'check-levels--dropdown-item %LEVEL%\']';
const confDeadmanCheckLevelsDropdownSelected = '[data-testid^=check-levels--dropdown--button] [class*=\'selected\'] [class*=\'dropdown--name\']';


const urlCtx = 'checks';

class checkEditPage extends influxPage {

    constructor(driver) {
        super(driver);
    }

    async isLoaded(){
        await super.isLoaded([
            {type: 'css', selector: dismissButton},
            {type: 'css', selector: pageCheckEditTitle},
            {type: 'css', selector: queriesToggle},
            {type: 'css', selector: configureCheckToggle}
        ], urlCtx);
    }

    static getOverlaySelector(){
        return { type: 'css', selector: overlay};
    }

    async getSaveCellButton(){
        return await this.driver.findElement(By.css(saveCellButton));
    }

    async getDismissButton(){
        return await this.driver.findElement(By.css(dismissButton));
    }

    async getPageCheckEditTitle(){
        return await this.driver.findElement(By.css(pageCheckEditTitle));
    }

    async getQueriesToggle(){
        return await this.driver.findElement(By.css(queriesToggle));
    }

    async getConfigureCheckToggle(){
        return await this.driver.findElement(By.css(configureCheckToggle))
    }

    async getDefineQueryToggle(){
        return await this.driver.findElement(By.css(defineQueryToggle));
    }

    async getChecklistPopover(){
        return await this.driver.findElement(By.css(checklistPopover));
    }

    async getPageCheckEditTitleInput(){
        return await this.driver.findElement(By.css(pageCheckEditTitleInput));
    }

    async getConfChkIntervalInput(){
        return await this.driver.findElement(By.xpath(confChkIntervalInput));
    }

    async getConfChkOffset(){
        return await this.driver.findElement(By.xpath(confChkOffset));
    }

    async getConfChkAddTagButton(){
        return await this.driver.findElement(By.xpath(confChkAddTagButton));
    }

    async getConfChkMessageTextArea(){
        return await this.driver.findElement(By.css(confChkMessageTextArea));
    }

    async getConfChkAddThresholdButton(status){
        return await this.driver.findElement(By.css(confChkAddThresholdButton.replace('%STATUS%', status)));
    }

    async getConfNthThresholdDefDropdownButton(index){
        return await this.driver.findElement(By.css(confNthThresholdDefDropdownButton
            .replace('%INDEX%', await this.getThresholdIndex(index))));
    }

    async getConfNthThresholdDefDropdownItem(index, item){
        return await this.driver.findElement(By.css(confNthThresholdDefDropdownItem
            .replace('%INDEX%',await this.getThresholdIndex(index)).replace('%ITEM%', item.toLowerCase())));
    }

    async getConfNthThresholdDefInput(index){
        return await this.driver.findElement(By.css(confNthThresholdDefInput.replace('%INDEX%', await this.getThresholdIndex(index))));
    }

    async getConfNthThreshold2ndInput(index){
        return await this.driver.findElement(By.css(confNthThreshold2ndInput.replace('%INDEX%', await this.getThresholdIndex(index))));
    }

    async getThresholdIndex(val){
        return await thresholds.indexOf(val.toUpperCase().trim()) + 1;
    }

    async getChecklistPopoverItemByText(text){
        return await this.driver.findElement(By.xpath(checklistPopoverItemByText.replace('%TEXT%', text.trim())));
    }

    async getConfDeadmanForInput(){
        return await this.driver.findElement(By.xpath(confDeadmanForInput));
    }

    async getConfDeadmanForInputDropdownItem(item){
        return await this.driver.findElement(By.xpath(confDeadmanForInputDropdownItem.replace('%ITEM%', item)));
    }

    async getConfDeadmanStopInput(){
        return await this.driver.findElement(By.xpath(confDeadmanStopInput));
    }

    async getConfDeadmanStopInputDropdownItem(item){
        return await this.driver.findElement(By.xpath(confDeadmanStopInputDropdownItem.replace('%ITEM%', item)));
    }

    async getConfDeadmanCheckLevelsDropdown(){
        return await this.driver.findElement(By.xpath(confDeadmanCheckLevelsDropdown));
    }

    async getConfDeadmanCheckLevelsDropodownItem(level){
        return await this.driver.findElement(By.css(confDeadmanCheckLevelsDropodownItem.replace('%LEVEL%', level)));
    }

    async getConfDeadmanCheckLevelsDropdownSelected(){
        return await this.driver.findElement(By.css(confDeadmanCheckLevelsDropdownSelected));
    }

    async getPreviewThresholdHandleByLevel(level){
        return await this.driver.findElement(By.css(previewThresholdHandleByLevel.replace('%LEVEL%', level.toLowerCase())));
    }

    async getConfTagRuleKeyInputOfTag(index){
        return await this.driver.findElement(By.css(confTagRuleKeyInputOfTag.replace('%INDEX%', parseInt(index) + 1)));
    }

    async getConfTagRuleValueInputOfTag(index){
        return await this.driver.findElement(By.css(confTagRuleValueInputOfTag.replace('%INDEX%', parseInt(index) + 1)));
    }

    async getConfTagRuleDimissOfTag(index){
        return await this.driver.findElement(By.css(confTagRuleDimissOfTag.replace('%INDEX%', parseInt(index) + 1)));
    }

}

module.exports = checkEditPage;
