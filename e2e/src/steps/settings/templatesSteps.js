const { expect } = require('chai');
const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const templatesTab = require(__srcdir + '/pages/settings/templatesTab.js');
const influxUtils = require(__srcdir + '/utils/influxUtils.js');

class templatesSteps extends baseSteps{

    constructor(driver){
        super(driver);
        this.tmTab = new templatesTab(driver);
    }

    async isLoaded(){
        await this.tmTab.isTabLoaded();
    }

    async verifyTemplateCardsSort(templates){
        let tempArray = templates.split(',');
        await this.tmTab.getTemplateCardNames().then(async cards => {
            for(let i = 0; i < tempArray.length; i++){
                expect(await cards[i].getText()).to.equal(tempArray[i]);
            }
        });
    }

    async verifyImportTemplatePopupLoaded(){
        await this.assertVisible(await this.tmTab.getImportTemplateUploadButton());
        await this.assertVisible(await this.tmTab.getImportTemplatePasteButton());
        await this.assertVisible(await this.tmTab.getPopupSubmit());
        await this.assertVisible(await this.tmTab.getPopupDismiss());
        await this.assertVisible(await this.tmTab.getPopupFileUploadHeader());
        await this.verifyElementContainsText(await this.tmTab.getPopupTitle(), 'Import Template');
    }

    async verifyImportTemplateFileUpload(present){
        if(present){
            await this.assertPresent(await this.tmTab.getPopupFileUploadSelector());
        }else{
            await this.assertNotPresent(await this.tmTab.getPopupFileUploadSelector());
        }
    }

    async verifyImportTemplatePasteJSON(present){
        if(present){
            await this.assertPresent(await templatesTab.getImportTemplateJSONTextAreaSelector());
        }else{
            await this.assertNotPresent(await templatesTab.getImportTemplateJSONTextAreaSelector());
        }
    }

    async clickUserTemplatesButton(){
        await this.clickAndWait(await this.tmTab.getUserTemplatesRadioButton());
    }

    async clickImportTemplateEmptyButton(){
        await this.clickAndWait(await this.tmTab.getImportTemplateEmptyButton());
    }

    async verifyImportTemplatePopupSubmitEnabled(enabled){
        if(enabled){
            await this.verifyElementEnabled(await this.tmTab.getPopupSubmit());
        }else{
            await this.verifyElementDisabled(await this.tmTab.getPopupSubmit());
        }
    }

    async clickImportTemplateHeaderButton(){
        await this.clickAndWait(await this.tmTab.getImportTemplateHeaderButton(),
            //seems to be overrunning ui response
            async () => { this.driver.sleep(1500);}); //todo better wait
    }

    async clickImportTemplatePasteButton(){
        await this.clickAndWait(await this.tmTab.getImportTemplatePasteButton(),
            async () => {  this.driver.sleep(1500);}); //todo better wait
    }

    async clickImportTemplateUploadButton(){
        await this.clickAndWait(await this.tmTab.getImportTemplateUploadButton());
    }

    async enterTextImportTemplateJSON(text){
        await this.typeTextAndWait(await this.tmTab.getImportTemplateJSONTextArea(), text);
    }

    //todo fix file separator in path below
    async uploadTemplateFile(filePath){
        await this.tmTab.getImportTemplateDragNDrop().then(async elem => {
            await elem.sendKeys(process.cwd() + '/' + filePath).then(async () => {
                await this.delay(200); //debug wait - todo better wait
            });
        });
    }

    async verifyTemplateCardVisibility(name){
        await this.assertVisible(await this.tmTab.getTemplateCardByName(name));
    }

    async copyFileContentsToTemplateTextare(filepath){
        await this.copyFileContentsToTextarea(filepath, await this.tmTab.getImportTemplateJSONTextArea());
    }

    async verifyRESTTemplateDocumentExists(user,title){
        //let uzzer = await influxUtils.getUser(user);
        let resp = await influxUtils.getDocTemplates(user);
        let match = resp.documents.filter( doc => doc.meta.name === title);
        expect(match.length).to.be.above(0);
    }

    async enterTemplatesFilterValue(value){
        await this.typeTextAndWait(await this.tmTab.getTemplatesFilter(), value);
    }

    async verifyTemplateNotPresent(name){
        await this.assertNotPresent(await templatesTab.getTemplateCardSelectorByName(name));
    }

    async clearTemplatesFilter(){
        await this.clearInputText(await this.tmTab.getTemplatesFilter());
    }

    async clickSortTemplatesByName(){
        await this.clickAndWait(await this.tmTab.getNameSort());
    }

    async hoverOverTemplateCard(name){
        await this.hoverOver(await this.tmTab.getTemplateCardByName(name));
    }

    async clickTemplateContextDelete(name){
        await this.clickAndWait(await this.tmTab.getTemplateCardCtxDelete(name));
    }

    async clickTemplateDeleteConfirm(name){
        await this.clickAndWait(await this.tmTab.getTemplateCardDeleteConfirm(name), async () => {
            await this.driver.sleep(1000); // todo better wait - occasional overrun
        });
    }
}

module.exports = templatesSteps;
