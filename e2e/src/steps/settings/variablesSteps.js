const { By } = require('selenium-webdriver');
const { expect } = require('chai');
const  path  = require('path');
const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const variablesTab = require(__srcdir + '/pages/settings/variablesTab.js');

class variablesSteps extends baseSteps{

    constructor(driver){
        super(driver);
        this.varTab = new variablesTab(driver);
    }

    async isLoaded(){
        await this.varTab.isTabLoaded();
    }

    async clickCreateVariableDropdown(){
        await this.clickAndWait(await this.varTab.getCreateVariableHeader());
    }

    async clickCreateVariableDropdownEmpty(){
        await this.clickAndWait(await this.varTab.getCreateVariableEmpty());
    }

    async clickCreateVariableDropdownItem(item){
        await this.clickAndWait(await this.varTab.getCreateVariableItem(item),
            //over running this step?
            async () => { await this.driver.sleep(1000); }); // todo better wait
    }

    async verifyImportVariablePopupLoaded(){
        await this.assertVisible(await this.varTab.getPopupDismiss());
        await this.assertVisible(await this.varTab.getUploadRadioButton());
        await this.assertVisible(await this.varTab.getPasteRadioButton());
        await this.assertPresent(await variablesTab.getDragNDropFileSelector()); //N.B. display: none
        await this.assertVisible(await this.varTab.getImportButton());
        await this.verifyElementContainsText(await this.varTab.getPopupTitle(), 'Import Variable');
    }

    async verifyCreateVariablePopupLoaded(){
        await this.assertVisible(await this.varTab.getPopupDismiss());
        await this.assertVisible(await this.varTab.getPopupCancelSimple());
        //await this.assertVisible(await this.varTab.getPopupCreate());
        await this.assertVisible(await this.varTab.getCreateVarPopupCreateButton());
        await this.assertVisible(await this.varTab.getCreateVariableNameInput());
        await this.assertVisible(await this.varTab.getCreateVariableTypeDropdown());
        await this.verifyElementContainsText(await this.varTab.getPopupTitle(), 'Create Variable');
    }

    async verifyEditVariablePopupLoaded(){
        await this.assertVisible(await this.varTab.getPopupDismiss());
        await this.assertVisible(await this.varTab.getPopupCancelSimple());
        await this.assertVisible(await this.varTab.getPopupSubmit());
        await this.assertVisible(await this.varTab.getCreateVariableNameInput());
        await this.assertVisible(await this.varTab.getEditVariableTypeDropdown());
        await this.verifyElementContainsText(await this.varTab.getPopupTitle(), 'Edit Variable');
    }

    async verifyVariableNameChangeWarningPopupLoaded(){
        await this.assertVisible(await this.varTab.getPopupDismiss());
        await this.assertVisible(await this.varTab.getEditVariablWarnSubmit());
        await this.verifyElementContainsText(await this.varTab.getPopupTitle(), 'Are you sure?');
        await this.varTab.getEditVariablWarnSubmit().then(async elem => {
            await this.verifyElementContainsText(await elem.findElement(By.xpath('./span')),
                'I understand, let\'s rename my Variable');
        });
    }

    async clickImportPopupPasteJSON(){
        await this.clickAndWait(await this.varTab.getPasteRadioButton());
    }

    async verifyImportPopupJSONTextareaVisible(visible){
        if(visible){
            await this.assertVisible(await this.varTab.getPasteJSONTextarea());
        }else{
            await this.assertNotPresent(variablesTab.getPasteJSONTextareaSelector());
        }
    }

    async verifyImportPopupImportJSONButtonEnabled(enabled) {
        if (enabled) {
            await this.verifyElementEnabled(await this.varTab.getImportButton());
        }else{
            await this.verifyElementDisabled(await this.varTab.getImportButton());
        }
    }

    async clickImportPopupUploadFile(){
        await this.clickAndWait(await this.varTab.getUploadRadioButton());
    }

    async verifyImportPopupFileUploadPresent(){
        await this.assertPresent(await variablesTab.getDragNDropFileSelector());
    }

    async verifyCreateVarPopupSelectedType(type){
        await this.varTab.getCreateVariableTypeDropdown().then(async elem => {
            await this.verifyElementText(await elem.findElement(By.css('[class*=selected]')), type);
        });
    }

    async verifyCreatePopupCreateEnabled(enabled){
        if(enabled) {
            await this.verifyElementEnabled(await this.varTab.getCreateVarPopupCreateButton());
        }else{
            await this.verifyElementDisabled(await this.varTab.getCreateVarPopupCreateButton());
        }
    }

    async verifyCreateVarPopupQueryEditorVisible(visible){
        if(visible){
            //await this.assertVisible(await this.varTab.getCreateVariableQueryCodeMirror());
            await this.assertVisible(await this.varTab.getCreateVariableQueryMonacoEdit());
        }else{
            //await this.assertNotPresent(await variablesTab.getCreateVariableQueryCodeMirrorSelector());
            await this.assertNotPresent(await variablesTab.getCreateVariableQueryMonacoEditSelector());
        }
    }

    async clickCreateVariableTypeDropdown(){
        await this.clickAndWait(await this.varTab.getCreateVariableTypeDropdown(),
            //seems to be slow
            async () => { this.driver.sleep(1000); }); //todo better wait
    }

    async clickEditVariableTypeDropdown(){
        await this.clickAndWait(await this.varTab.getEditVariableTypeDropdown());
    }

    async clickCreateVarPopupTypeDropdownItem(item){
        await this.clickAndWait(await this.varTab.getCreateVariableTypeDropdownItem(item));
    }

    async clickEditVarPopupTypeDropdownItem(item){
        await this.clickAndWait(await this.varTab.getEditVariableTypeDropdownItem(item));
    }

    async verifyCreateVarPopupTextareaVisible(visible){
        if(visible){
            await this.assertVisible(await this.varTab.getCreateVariableTextArea());
        }else{
            await this.assertNotPresent(variablesTab.getCreateVariableTextAreaSelector());
        }
    }

    async verifyCreateVarPopupDefaultValDropdownVisible(visible){
        if(visible){
            await this.assertVisible(await this.varTab.getCreateVariableDefaultValDropdown());
        }else{
            await this.assertNotPresent(variablesTab.getCreateVariableDefaultValDropdownSelector());
        }
    }

    async verifyCreateVarPopupInfoCount(count){
        await this.varTab.getCreateVariableInfoPara().then(async elem => {
            await elem.getText().then(async elText=> {
                // e.g. "Mapping Contains 0 key-value pairs"
                let tokens = elText.split(' ');
                expect(parseInt(tokens[2])).to.equal(parseInt(count));
            });
        });
    }

    async verifyCreateVarPopupInfoVisible(visible){
        if(visible){
            await this.assertVisible(await this.varTab.getCreateVariableInfoPara());
        }else{
            await this.assertNotPresent(variablesTab.getCreateVariableInfoParaSelector());
        }
    }

    async uploadImportVarPopupFile(path2file){
        await this.varTab.getDragNDropFile().then(async elem => {
            await elem.sendKeys(process.cwd() + '/' + path2file).then(async () => {
                await this.delay(200); //debug wait - todo better wait
            });
        });
    }

    async verifyImportPopupUploadSuccess(){
        await this.verifyElementContainsClass(await this.varTab.getImportVariableDragNDropHeader(), 'selected');
    }

    async verifyImportPopupUploadFilename(path2file){
        let fp = path.parse(path2file);
        await this.verifyElementContainsText(await this.varTab.getImportVariableDragNDropHeader(), fp.base);
    }

    async clickImportPopupImportButton(){
        await this.clickAndWait(await this.varTab.getImportButton(), async () => {
            await this.delay(1500); //lengthen a bit - sometimes slow to import - todo better wait
        });
    }

    async verifyVariableCardVisible(name){
        await this.assertVisible(await this.varTab.getVariableCardNamed(name));
    }

    async enterCreateVarPopupName(name){
        await this.typeTextAndWait(await this.varTab.getCreateVariableNameInput(), name);
    }

    async clearCreateVarPopupName(){
        await this.clearInputText(await this.varTab.getCreateVariableNameInput());
    }

    async enterCreateVarPopupTextarea(values){
        await this.typeTextAndWait(await this.varTab.getCreateVariableTextArea(), values);
    }

    async verifyCreatePopupDefaultValDropdownSelected(item){
        await this.varTab.getCreateVariableDefaultValDropdown().then(async elem => {
            await elem.findElement(By.css('[class*=\'selected\']')).then(async el2 => {
                await this.verifyElementContainsText(el2, item);
            });
        });
    }

    async clickCreateVarPopupDefaultDropdown(){
        await this.clickAndWait(await this.varTab.getCreateVariableDefaultValDropdown());
    }

    async clickCreateVarPopupDefaultDropdownItem(item){
        await this.clickAndWait(await this.varTab.getCreateVariableDefaultValDropdownItem(item));
    }

    async clickCreatVarPopupDefaultCSVDropdownItem(item){
        await this.clickAndWait(await this.varTab.getCreateVariableDefaultValCSVDropdownItem(item));
    }

    async clickCreateVarPopupCreateButton(){
        await this.clickAndWait(await this.varTab.getCreateVarPopupCreateButton());
    }

    async clickEditVarPopupSubmitButton(){
        await this.clickAndWait(await this.varTab.getPopupSubmit());
    }

    async setVariablePopupCodeMirrorText(text){
        await this.setCodeMirrorText(await this.varTab.getCreateVariableQueryCodeMirror(), text);
    }

    async setVariablePopupMonacoEditText(text){
        await this.setMonacoEditorText(await this.driver.findElement(By.css('.monaco-editor .inputarea')),
            text);
    }

    async enterValueIntoVariablesFilter(value){
        await this.typeTextAndWait(await this.varTab.getVariablesFilter(), value);
    }

    async verifyVariableCardsVisible(cards){
        let cardsArr = cards.split(',');
        for(let i = 0; i < cardsArr.length; i++){
            await this.assertVisible(await this.varTab.getVariableCardNamed(cardsArr[i]));
        }
    }

    async verifyVariablsCardsNotPresent(cards){
        let cardsArr = cards.split(',');
        for(let i = 0; i < cardsArr.length; i++){
            await this.assertNotPresent(await variablesTab.getVariableCardSelectorByName(cardsArr[i]));
        }
    }

    async verifyVariableCardsSort(cards){
        let cardsArr = cards.split(',');
        await this.varTab.getVariableCardNames().then(async cardNames => {
            for(let i = 0; i < cardsArr.length; i++){
                expect(await cardNames[i].getText()).to.equal(cardsArr[i]);
            }
        });
    }

    async clickVariableSortByName(){
        await this.clickAndWait(await this.varTab.getNameSort());
    }

    async clearVariablesFilter(){
        await this.clearInputText(await this.varTab.getVariablesFilter());
    }

    async clickVariableCardName(name){
        await this.clickAndWait(await this.varTab.getVariableCardName(name));
    }

    async hoverOverVariableCard(name){
        await this.hoverOver(await this.varTab.getVariableCardNamed(name));
    }

    async clickVariableCardContextMenu(name){
        await this.clickAndWait(await this.varTab.getVariableCardContextMenu(name));
    }

    async clickVariableCardContextMenuItem(name,item){
        await this.clickAndWait(await this.varTab.getVariableCardContextMenuItem(name, item),
            //troubleshoot occasional overrun
            async () => { await this.driver.sleep(2000); } ); //todo better wait
    }

    async clickVariableNameChangeWarningUnderstand(){
        await this.clickAndWait(await this.varTab.getEditVariablWarnSubmit(),
            //occasional overrun at this point
            async () => { await this.driver.sleep(2000); }); //todo better wait
    }

    async clearVariableNameChangeNameInput(){
        await this.clearInputText(await this.varTab.getUpdateNameNameInput());
    }

    async enterNewVariableName(name){
        await this.typeTextAndWait(await this.varTab.getUpdateNameNameInput(), name);
    }

    async verifyChangeVariableNameSubmitDisabled(){
        await this.verifyElementDisabled(await this.varTab.getEditVariableNameChangeSubmit());
    }

    async clickSubmitRenameVariablePopup(){
        await this.clickAndWait(await this.varTab.getEditVariableNameChangeSubmit());
    }

    async verifyEditVariablePopupNameDisabled(){
        await this.verifyElementContainsAttribute(await this.varTab.getEditVariableNameInput(), 'disabled');
    }

    async verifyEditVariablePopupTextareaCleared(){
        await this.verifyElementContainsNoText(await this.varTab.getCreateVariableTextArea());
    }

    async clickVariableCardDelete(name){
        await this.clickAndWait(await this.varTab.getVariableCardContextDelete(name));
    }

    async clickVariableCardDeleteConfirm(name){
        await this.clickAndWait(await this.varTab.getVariableCardContextDeleteConfirm(name));
    }

    async verifyVariableCardNotPresent(name){
        await this.assertNotPresent(await variablesTab.getVariableCardSelectorByName(name));
    }
}



module.exports = variablesSteps;
