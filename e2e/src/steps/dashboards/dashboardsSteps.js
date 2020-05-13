const { expect } = require('chai');
const  path  = require('path');

const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const dashboardsPage = require(__srcdir + '/pages/dashboards/dashboardsPage.js');
const influxUtils = require(__srcdir + '/utils/influxUtils.js');

class dashboardsSteps extends influxSteps {

    constructor(driver){
        super(driver);
        this.dbdsPage = new dashboardsPage(driver);
    }

    async isLoaded(){
        await this.dbdsPage.isLoaded();
    }

    async verifyIsLoaded(){
        this.assertVisible(await this.dbdsPage.getCreateDashboardDropdown());
        this.assertVisible(await this.dbdsPage.getFilterDashboards());
        this.assertVisible(await this.dbdsPage.getSortTypeButton());
      //  this.assertVisible(await this.dbdsPage.getModifiedSortButton());
    }

    async clickCreateDashboard(){
        await this.clickAndWait(await this.dbdsPage.getCreateDashboardDropdown()); // todo better wait
    }

    async clickCreateDashboardItem(item){
        await this.clickAndWait(await this.dbdsPage.getCreateDashboardItem(item),
            async () => { await this.driver.sleep(1000);}); // todo better wait - slow to load?
    }

    async clickCreateDashboardEmpty(){
        await this.clickAndWait(await this.dbdsPage.getCreateDashboardDropdownEmpty()); // todo better wait
    }

    async verifyEmptyCreateDashboardItems(items){
        let itemsArr = items.split(',');
        await this.dbdsPage.getCreateDashboardItems().then( async pgItems => {
            for( let i = 0; i < pgItems.length; i++){
                expect(await pgItems[i].getAttribute('id')).to.equal(itemsArr[i].trim());
            }
        });
    }

    async verifyEmptyCreateDashboardNotPresent(){
        await this.assertNotPresent(dashboardsPage.getCreateDashboardDropdownEmptySelector());
    }

    async verifyDashboardCardVisible(name){
        await this.assertVisible(await this.dbdsPage.getDashboardCardByName(name));
    }

    async verifyDashboardCardNotPresent(name){
        await this.assertNotPresent(await dashboardsPage.getDashboardCardSelectorByName(name));
    }

    async hoverOVerDashboardCard(name){
        await this.hoverOver(await this.dbdsPage.getDashboardCardByName(name));
    }

    async verifyExportButtonOfCardVisible(name){
        await this.assertVisible(await this.dbdsPage.getDashboardCardExportButton(name));
    }

    async verifyCloneButtonOfCardVisible(name){
        await this.assertVisible(await this.dbdsPage.getDashboardCardCloneButton(name));
    }

    async verifyDeleteButtonOfCardVisible(name){
        await this.assertVisible(await this.dbdsPage.getDashboardCardDeleteButton(name));
    }

    async hoverOverDashboardCardName(name){
        await this.hoverOver(await this.dbdsPage.getDashboardCardName(name));
    }

    async clickDashboardCardName(name){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardName(name), async () => {
            await this.driver.sleep(500); //troubleshoot why dashboard not loaded
        });
    }

    async clickDashboardCardNameButton(name){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardNameButton(name));
    }

    async clearDashboardCardName(name){
        await this.clearInputText(await this.dbdsPage.getDashboardCardNameInput(name));
    }

    async renameDashboardCard(newName, oldName){
        await this.typeTextAndWait(await this.dbdsPage.getDashboardCardNameInput(oldName), newName);
    }

    async verifyDashboardCardContainsDescription(name,descr){
        await this.verifyElementContainsText(await this.dbdsPage.getDashboardCardDescription(name),
            descr);
    }

    async hoverOverDashboardCardDescription(name){
        await this.hoverOver(await this.dbdsPage.getDashboardCardDescription(name));
    }

    async clickDashboardCardEditDescriptionButton(name){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardDescriptionEdit(name));
    }

    async enterDashboardCardDescription(name,descr){
        await this.typeTextAndWait(await this.dbdsPage.getDashboardCardDescriptionInput(name), descr);
    }

    async clickEmptyLabelOfDashboardCard(name){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardLabelsEmpty(name));
    }

    async clickAddLabelOfDashboardCard(name){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardAddLabels(name));
    }

    async verifyLabelInDashboardsPopoverIsVisible(label){
        await this.assertVisible(await this.dbdsPage.getAddLabelsPopoverLabel(label));
    }

    async verifyLabelInDashboardsPopoverIsNotPresent(label){
        await this.assertNotPresent(await dashboardsPage.getAddLabelsPopoverLabelSelector(label));
    }

    async enterDashboardLabelsFilter(text){
        await this.typeTextAndWait(await this.dbdsPage.getAddLabelsPopoverFilter(), text);
    }

    async verifyDasboardAddLabelsPillCount(count){
        await this.dbdsPage.getAddLabelsLabelPills().then(async pills => {
            expect(pills.length).to.equal(parseInt(count));
        });
    }

    async verifyLabelPopoverCreateNewNotPresent(){
        await this.assertNotPresent(await dashboardsPage.getAddLabelsPopoverNewItemSelector());
    }

    async verifyLabelPopoverCreateNewIsVisible(){
        await this.assertVisible(await this.dbdsPage.getAddLabelsPopoverNewItem());
    }

    async clickLabelPopoverCreateNewLabel(){
        await this.clickAndWait(await this.dbdsPage.getAddLabelsPopoverNewItem(),
            async () => { await this.driver.sleep(1000); }); //popup slow to load? todo better wait
    }

    async verifyDashboardCardHasLabel(name, label){
        await this.assertVisible(await this.dbdsPage.getDashboardCardLabelPill(name, label));
    }

    async clickDasboardCardAddLabel(name){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardAddLabels(name));
    }

    async hoverDashboardCardLabel(name, label){
        await this.hoverOver(await this.dbdsPage.getDashboardCardLabelPill(name, label));
    }

    async clickDashboardCardRemoveLabel(name,label){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardLabelPillDelete(name, label));
    }

    async verifyDashboardCardLabelsEmptyVisible(name){
        await this.assertVisible(await this.dbdsPage.getDashboardCardLabelsEmpty(name));
    }

    async verifyDashboardCardLabelNotPresent(name, label){
        await this.assertNotPresent(await dashboardsPage.getDashboardCardLabelPillSelector(name, label));
    }

    async verifyDashboardCardsVisible(cards){
        let cardsArr = cards.split(',');
        cardsArr.forEach(async cardName => {
            await this.assertVisible(await this.dbdsPage.getDashboardCardByName(cardName));
        } );
    }

    async verifyDashboardCardsNotPresent(cards){
        let cardsArr = cards.split(',');
        for(let i = 0; i < cardsArr.length; i++){
            await this.assertNotPresent(await dashboardsPage.getDashboardCardSelectorByName(cardsArr[i].trim()));
        }
    }

    async enterDashboardsCardFilter(term){
        await this.typeTextAndWait(await this.dbdsPage.getFilterDashboards(), term);
    }

    async clearDashboardsCardFilter(){
        await this.clearInputText(await this.dbdsPage.getFilterDashboards());
    }

    async verifyImportDashboardPopupVisible(){
        await this.verifyElementContainsText(await this.dbdsPage.getPopupTitle(), 'Import Dashboard');
        //and upload file
        await this.assertVisible(await this.dbdsPage.getImportPopupUploadFileRadio());
        //and paste json
        await this.assertVisible(await this.dbdsPage.getImportPopupPasteJSONRadio());
        //and dismiss button
        await this.assertVisible(await this.dbdsPage.getImportPopupDismiss());
        //and Import json button
        await this.assertVisible(await this.dbdsPage.getImportPopupImportJSONButton());
        //and drag and drop form
        await this.assertVisible(await this.dbdsPage.getImportPopupFileInput());
    }

    async verifyCreateDashboardFromTemplatePopupVisible(){
        await this.verifyElementContainsText(await this.dbdsPage.getPopupTitle(), 'Create Dashboard from a Template');
        //dismiss
        await this.assertVisible(await this.dbdsPage.getFromTemplatePopupDismiss());
        //cancel
        await this.assertVisible(await this.dbdsPage.getFromTemplatePopupCancel());
        //create Dashboard button
        await this.assertVisible(await this.dbdsPage.getFromTemplatePopupCreateDBoard());
        //templates list
        await this.assertVisible(await this.dbdsPage.getFromTemplatePopupTemplateList());
        //templates list item
        await this.assertVisible(await this.dbdsPage.getFromTemplatePopupTemplateItem('System'));
        //template panel - preview
        await this.assertVisible(await this.dbdsPage.getFromTemplatePopupTemplatePanel());
    }

    async uploadImportDashboardPopupFile(path2file){
        await this.dbdsPage.getImportPopupDragNDropFile().then(async elem => {
            await elem.sendKeys(process.cwd() + '/' + path2file).then(async () => {
                await this.delay(200); //debug wait - todo better wait
            });
        });
    }

    async verifyImportPopupUploadSuccess(){
        await this.verifyElementContainsClass(await this.dbdsPage.getImportPopupFileInputHeader(), 'selected');
    }

    async verifyImportPopupUploadFilename(filepath){
        let fp = path.parse(filepath);
        await this.verifyElementContainsText(await this.dbdsPage.getImportPopupFileInputHeader(), fp.base);
    }

    async clickImportDashboardButton(){
        await this.clickAndWait(await this.dbdsPage.getImportPopupImportJSONButton());
    }

    async clickImportDashboardPasteJSON(){
        await this.clickAndWait(await this.dbdsPage.getImportPopupPasteJSONRadio());
    }

    async verifyImportDashboardFileUploadNotPresent(){
        await this.assertNotPresent(await dashboardsPage.getImportPopupFileInputSelector());
    }

    async pasteFileContentsImportDashboardTextarea(filepath){
        let contents = await influxUtils.readFileToBuffer(filepath);
        //console.log("DEBUG file contents:\n " + contents );
        await this.typeTextAndWait(await this.dbdsPage.getImportPopupJSONTextarea(), contents);
    }

    async clickFromTemplatePopupCancel(){
        await this.clickAndWait(await this.dbdsPage.getFromTemplatePopupCancel());
    }

    async clickFromTemplatePopupTemplateItem(item){
        await this.clickAndWait(await this.dbdsPage.getFromTemplatePopupTemplateItem(item));
    }

    async verifyFromTemplatePreviewCellVisible(name){
        await this.assertVisible(await this.dbdsPage.getfromTemplatePopupPreviewCell(name));
    }

    async verifyFromTemplatePopupCreateDisabled(){
        await this.verifyElementDisabled(await this.dbdsPage.getFromTemplatePopupCreateDBoard());
    }

    async clickDashboardFromTemplateCreate(){
        await this.clickAndWait(await this.dbdsPage.getFromTemplatePopupCreateDBoard());
    }

    async verifyDashboardSortOrder(dBoards){
        let dbArray = dBoards.split(',');
        await this.dbdsPage.getDashboardCardNames().then(async names => {
            for(let i = 0; i < names.length; i++){
                expect(await names[i].getText()).to.equal(dbArray[i]);
            }
        });
    }

    async clickSortDashboardsListItem(item){
        await this.clickAndWait(await this.dbdsPage.getSortTypeItem(item.toLowerCase()
            .replace(" ", "-")));
    }

    async clickSortDashboardsByType(){
        await this.clickAndWait(await this.dbdsPage.getSortTypeButton());
    }

    async clickDashboardCardDelete(name){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardDeleteButton(name));
    }

    async clickDashboardCardDeleteConfirm(name){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardDeleteConfirm(name));
    }

    async clickDashboardCardClone(name){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardCloneButton(name));
    }

    async clickDashboardCardCloneConfirm(name){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardCloneConfirm(name));
    }

    async clickDashboardCardExport(name){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardExportButton(name));
    }

    async clickDashboardCardExportConfirm(name){
        await this.clickAndWait(await this.dbdsPage.getDashboardCardExportConfirm(name),
            async () => { await this.driver.sleep(1000); }); //slow to load?
    }

    async verifyExportDashboardPopupLoaded(){
        await this.verifyElementContainsText(await this.dbdsPage.getPopupTitle(), 'Export Dashboard');
        //dismiss
        await this.assertVisible(await this.dbdsPage.getExportPopupDismiss());
        //codeMirror
        await this.assertVisible(await this.dbdsPage.getExportPopupCodeMirror());
        //DownloadJSON
        await this.assertVisible(await this.dbdsPage.getExportPopupDownloadJSON());
        //AsTemplate
        await this.assertVisible(await this.dbdsPage.getExportPopupSaveAsTemplate());
        //Copy2Clipboard
        await this.assertVisible(await this.dbdsPage.getexportPopupCopyToClipboard());
    }

    async clickExportDashboardPopupDismiss(){
        await this.clickAndWait(await this.dbdsPage.getExportPopupDismiss());
    }

    async clickExportDashboardDownloadJSON(filePath){
        if(__config.sel_docker){
            console.warn('File export not supported without shared memory');
            await this.clickAndWait(await this.dbdsPage.getExportPopupDownloadJSON());
            return;
        }
        await this.clickAndWait(await this.dbdsPage.getExportPopupDownloadJSON(),
            async () => {await influxUtils.waitForFileToExist(filePath); }); //wait for download to complete
    }

    async clickExportDashboardSaveAsTemplate(){
        await this.clickAndWait(await this.dbdsPage.getExportPopupSaveAsTemplate());
    }

    async clickExportDashboardCopyToClipboard(){
        await this.clickAndWait(await this.dbdsPage.getexportPopupCopyToClipboard());
    }

    async clickDashboardsFilterInput(){
        await this.clickAndWait(await this.dbdsPage.getFilterDashboards());
    }

}

module.exports = dashboardsSteps;

