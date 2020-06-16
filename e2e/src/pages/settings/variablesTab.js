const { By } = require('selenium-webdriver');
const settingsPage = require(__srcdir + '/pages/settings/settingsPage.js');
const basePage = require(__srcdir + '/pages/basePage.js');

const variablesFilter = '[data-testid=search-widget]';
const createVariableHeader = '[data-testid=\'tabbed-page--header\'] [data-testid=add-resource-dropdown--button]';
const nameSort = '[data-testid=resource-list--sorter]:nth-of-type(1)';
const typeSort = '[data-testid=resource-list--sorter]:nth-of-type(2)';
const createVariableEmpty = '[data-testid=resource-list--body] [data-testid=add-resource-dropdown--button]';
const createVariableItem = '[data-testid=add-resource-dropdown--%ITEM%]';
const variableCardNamed = '//*[@data-testid=\'resource-card variable\'][.//*[text()=\'%NAME%\']]';
const variableCardNames = '[data-testid^=\'variable-card--name\'] span';
const variableCardName = '//*[@data-testid=\'variable-card--name %NAME%\']//span[text()=\'%NAME%\']';
const variableCardContextMenu = '//*[@data-testid=\'resource-card variable\'][.//span[text()=\'%NAME%\']]//*[@data-testid=\'context-menu\']';
const variableCardContextMenuItem = '//*[@data-testid=\'resource-card variable\'][.//span[text()=\'%NAME%\']]//*[button[@data-testid=\'context-menu\']]//button[text()=\'%ITEM%\']';
const variableCardContextDelete = '//*[@data-testid=\'resource-card variable\'][.//span[text() = \'%NAME%\']]//*[@data-testid=\'context-delete-menu\']';
const variableCardContextDeleteConfirm = '//*[@data-testid=\'resource-card variable\'][.//span[text() = \'%NAME%\']]//*[@data-testid=\'context-delete-variable\']';

const urlCtx = 'variables';

// import variable popup
const uploadRadioButton = '[data-testid=overlay--body] [data-testid=select-group--option][title=Upload]';
const pasteRadioButton = '[data-testid=overlay--body] [data-testid=select-group--option][title=Paste]';
const dragNDropFile = 'input[type=file]'; //N.B. has display:none
const importButton = '[data-testid=overlay--footer] [data-testid=button]';
const pasteJSONTextarea = '[data-testid=overlay--body] [data-testid=import-overlay--textarea]';
const importVariableDragNDropHeader = '.drag-and-drop--header';

// create variable popup
const createVarPopupCreateButton = '[data-testid=overlay--container] button[title=\'Create Variable\']';
const createVariableNameInput = '[data-testid=overlay--body] [data-testid=input-field]';
const createVariableTypeDropdown = '[data-testid=\'variable-type-dropdown--button\']';
const createVariableQueryCodeMirror = '.CodeMirror';
const createVariableQueryMonacoEdit = '.monaco-editor';
const createVariableTextArea = '[data-testid=overlay--body] [data-testid=textarea]';
const createVariableTypeDropdownItem = '[data-testid=\'variable-type-dropdown-%ITEM%\']';
const createVariableDefaultValDropdown = '//*[@data-testid=\'form--element\'][.//*[text() = \'Select A Default\']]//*[contains(@data-testid,\'dropdown--button\')]';
const createVariableInfoPara = '//*[@data-testid=\'grid--column\'][p[contains(text(), \'ontains\')]]';
const createVariableDefaultValDropdownItem = '[data-testid=dropdown-item][id=\'%ITEM%\']';
const createVariableDefaultValCSVDropdownItem = '//*[@data-testid=\'dropdown-item\']//*[text() = \'%ITEM%\']';

// edit variable popup
const editVariableTypeDropdown = '[data-testid=\'variable-type-dropdown--button\']';
const editVariableTypeDropdownItem = '[data-testid=\'variable-type-dropdown-%ITEM%\']';
const editVariableNameInput = '//*[@data-testid=\'form--element\'][.//*[text()=\'Name\']]//input';
const editWarnVariablSubmit = '[data-testid=danger-confirmation-button]';
const editVariableNameChangeSubmit = '[data-testid=rename-variable-submit]';

//Warning popup
const updateNameNameInput = '[data-testid=overlay--body] [data-testid=rename-variable-input]';


class variablesTab extends settingsPage{

    constructor(driver){
        super(driver);
    }

    async isTabLoaded(){
        await super.isTabLoaded(urlCtx,
            [
                {type: 'css', selector: variablesFilter},
                {type: 'css', selector: createVariableHeader},
                basePage.getSortTypeButtonSelector()
                //{type: 'css', selector: nameSort},
                //{type: 'css', selector: typeSort},
            ]
        );
    }

    async getVariablesFilter(){
        return await this.driver.findElement(By.css(variablesFilter));
    }

    async getCreateVariableHeader(){
        return await this.driver.findElement(By.css(createVariableHeader));
    }

    async getCreateVariableEmpty(){
        return await this.driver.findElement(By.css(createVariableEmpty));
    }

    async getCreateVariableItem(item){
        return await this.driver.findElement(By.css(createVariableItem.replace('%ITEM%', item)));
    }

    async getPasteRadioButton(){
        return await this.driver.findElement(By.css(pasteRadioButton));
    }

    async getUploadRadioButton(){
        return await this.driver.findElement(By.css(uploadRadioButton));
    }

    async getDragNDropFile(){
        return await this.driver.findElement(By.css(dragNDropFile));
    }

    static getDragNDropFileSelector(){
        return { type: 'css', selector: dragNDropFile};
    }

    async getImportButton(){
        return await this.driver.findElement(By.css(importButton));
    }

    async getCreateVariableNameInput(){
        return await this.driver.findElement(By.css(createVariableNameInput));
    }

    async getCreateVariableTypeDropdown(){
        return await this.driver.findElement(By.css(createVariableTypeDropdown));
    }

    async getCreateVariableQueryCodeMirror(){
        return await this.driver.findElement(By.css(createVariableQueryCodeMirror));
    }

    static getCreateVariableQueryCodeMirrorSelector(){
        return {type: 'css', selector: createVariableQueryCodeMirror };
    }

    async getCreateVariableQueryMonacoEdit(){
        return await this.driver.findElement(By.css(createVariableQueryMonacoEdit));
    }

    static getCreateVariableQueryMonacoEditSelector(){
        return {type: 'css', selector: createVariableQueryMonacoEdit};
    }

    async getCreateVariableTextArea(){
        return await this.driver.findElement(By.css(createVariableTextArea));
    }

    static getCreateVariableTextAreaSelector(){
        return {type: 'css', selector: createVariableTextArea};
    }

    async getPasteJSONTextarea(){
        return await this.driver.findElement(By.css(pasteJSONTextarea));
    }

    static getPasteJSONTextareaSelector(){
        return { type: 'css', selector: pasteJSONTextarea};
    }

    async getCreateVariableTypeDropdownItem(item){
        return await this.driver.findElement(By.css(createVariableTypeDropdownItem
            .replace('%ITEM%', item.toLowerCase())));
    }

    async getCreateVariableDefaultValDropdown(){
        return await this.driver.findElement(By.xpath(createVariableDefaultValDropdown));
    }

    static getCreateVariableDefaultValDropdownSelector(){
        return { type: 'xpath', selector: createVariableDefaultValDropdown};
    }

    async getCreateVariableInfoPara(){
        return await this.driver.findElement(By.xpath(createVariableInfoPara));
    }

    static getCreateVariableInfoParaSelector(){
        return { type: 'xpath', selector: createVariableInfoPara};
    }

    async getImportVariableDragNDropHeader(){
        return await this.driver.findElement(By.css(importVariableDragNDropHeader));
    }

    async getVariableCardNamed(name){
        return await this.driver.findElement(By.xpath(variableCardNamed.replace('%NAME%', name)));
    }

    static getVariableCardSelectorByName(name){
        return { type: 'xpath', selector: variableCardNamed.replace('%NAME%', name)};
    }

    async getCreateVariableDefaultValDropdownItem(item){
        return await this.driver.findElement(By.css(createVariableDefaultValDropdownItem.replace('%ITEM%', item)));
    }

    async getCreateVariableDefaultValCSVDropdownItem(item){
        return await this.driver.findElement(By.xpath(createVariableDefaultValCSVDropdownItem.replace('%ITEM%', item)));
    }

    async getVariableCardNames(){
        return await this.driver.findElements(By.css(variableCardNames));
    }

    async getVariableCardName(name){
        return await this.driver.findElement(By.xpath(variableCardName.replace(/%NAME%/g, name)));
    }

    async getNameSort(){
        return await this.driver.findElement(By.css(nameSort));
    }


    async getVariableCardContextMenu(name){
        return await this.driver.findElement(By.xpath(variableCardContextMenu.replace('%NAME%', name)));
    }

    async getVariableCardContextMenuItem(name, item){
        return await this.driver.findElement(By.xpath(variableCardContextMenuItem
            .replace('%NAME%', name)
            .replace('%ITEM%', item)));
    }

    async getUpdateNameNameInput(){
        return await this.driver.findElement(By.css(updateNameNameInput));
    }

    async getEditVariableTypeDropdown(){
        return await this.driver.findElement(By.css(editVariableTypeDropdown));
    }


    async getEditVariableTypeDropdownItem(item){
        return await this.driver.findElement(By.css(editVariableTypeDropdownItem.replace('%ITEM%', item.toLowerCase())));
    }

    async getEditVariableNameInput(){
        return await this.driver.findElement(By.xpath(editVariableNameInput));
    }

    async getVariableCardContextDelete(name){
        return await this.driver.findElement(By.xpath(variableCardContextDelete.replace('%NAME%', name)));
    }

    async getVariableCardContextDeleteConfirm(name){
        return await this.driver.findElement(By.xpath(variableCardContextDeleteConfirm.replace('%NAME%', name)));
    }

    async getEditVariablWarnSubmit(){
        return await this.driver.findElement(By.css(editWarnVariablSubmit));
    }

    async getEditVariableNameChangeSubmit(){
        return await this.driver.findElement(By.css(editVariableNameChangeSubmit));
    }

    async getCreateVarPopupCreateButton(){
        return await this.driver.findElement(By.css(createVarPopupCreateButton));
    }

}

module.exports = variablesTab;
