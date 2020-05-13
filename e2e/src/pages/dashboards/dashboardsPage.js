const influxPage = require(__srcdir + '/pages/influxPage.js');
const { By } = require('selenium-webdriver');

const createDashboardDropdown = '[data-testid=add-resource-dropdown--button]';
const filterDashboards =  '[data-testid=search-widget]';
const sortTypeButton = '[data-testid=resource-sorter--button]:nth-of-type(1)';
const sortTypeItem = '[data-testid=\'resource-sorter--%ITEM%\']';
const modifiedSortButton = '[data-testid=resource-list--sorter]:nth-of-type(2)';
const createDashboardDropdownEmpty = '[data-testid=\'page-contents\'] [data-testid=\'add-resource-dropdown--button\']';
const createDashboardItems = '[data-testid^=add-resource-dropdown--][id]';
const dashboardCardByName = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]';
const dashboardCardExportButton = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@class=\'context-menu--container\'][.//*[text() = \'Export\']]';
const dashboardCardExportConfirm = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[text()=\'Export\']';
const dashboardCardCloneButton = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@class=\'context-menu--container\'][.//*[text() = \'Clone\']]';
const dashboardCardCloneConfirm = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@class=\'context-menu--container\']//*[text() = \'Clone\']';
const dashboardCardDeleteButton = '//*[@data-testid=\'dashboard-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'context-delete-menu\']';
const dashboardCardDeleteConfirm = '//*[@data-testid=\'dashboard-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'context-delete-dashboard\']';
//const dashboardCardName = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@data-testid=\'dashboard-card--name\']';
const dashboardCardName = '//*[@data-testid=\'dashboard-card\']//span[text() = \'%NAME%\']';
const dashboardCardNames = '[data-testid=\'dashboard-card--name\']';
const dashboardCardNameButton = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@data-testid=\'dashboard-card--name-button\']';
const dashboardCardNameInput = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@data-testid=\'dashboard-card--input\']';
const dashboardCardDescription = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@data-testid=\'resource-list--editable-description\']';
const dashboardCardDescriptionEdit = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@data-testid=\'resource-list--editable-description\']//*[@data-testid=\'icon\']';
const dashboardCardDescriptionInput = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@data-testid=\'resource-list--editable-description\']//*[@data-testid=\'input-field\']';
const dashboardCardLabelsEmpty = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@data-testid=\'inline-labels--empty\']';
const dashboardCardAddLabels = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@data-testid=\'inline-labels--add\']';
const dashboardCardLabelPill = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@data-testid=\'label--pill %LABEL%\']';
const dashboardCardLabelPillDelete = '//*[@data-testid=\'dashboard-card\'][.//span[text() = \'%NAME%\']]//*[@data-testid=\'label--pill--delete %LABEL%\']';
////*[@data-testid='dashboard-card'][.//*[text()='Test Dashboard']]//*[@data-testid='context-menu']

const addLabelsPopover = '[data-testid=\'inline-labels--popover\']';
const addLabelsPopoverLabel = '//*[@data-testid=\'inline-labels--popover--contents\']//*[contains(@data-testid,\'label--pill\')][text()=\'%LABEL%\']';
const addLabelsPopoverFilter = '[data-testid=\'inline-labels--popover-field\']';
const addLabelsLabelPills = '[data-testid^=\'label--pill\']';
const addLabelsPopoverListItem = '[data-testid^=\'label-list--item %ITEM%\']';
const addLabelsPopoverNewItem = '[data-testid^=\'inline-labels--create-new\']';

const importPopupUploadFileRadio = '[data-testid=overlay--body] [data-testid=select-group--option][title=\'Upload\']';
const importPopupPasteJSONRadio = '[data-testid=overlay--body] [data-testid=select-group--option][title=\'Paste\']';
const importPopupImportJSONButton = '[data-testid=\'overlay--footer\'] [title^=\'Import JSON\']';
const importPopupDismiss = '[data-testid=\'overlay--header\'] button';
const importPopupFileInput = '[data-testid=\'overlay--body\'] [class*=\'drag-and-drop--form\'] ';
const importPopupFileInputHeader = '[data-testid=\'overlay--body\'] [class*=\'drag-and-drop--header\']';
const importPopupDragNDropFile = 'input[type=file]'; //N.B. has display:none
const importPopupJSONTextarea = '[data-testid=\'overlay--body\'] [data-testid=\'import-overlay--textarea\']';

const fromTemplatePopupDismiss = '[data-testid=\'overlay--header\'] button';
const fromTemplatePopupCancel = '[data-testid=\'overlay--footer\'] [data-testid=\'button\'][title=\'Cancel\']';
const fromTemplatePopupCreateDBoard = '[data-testid=\'create-dashboard-button\']';
const fromTemplatePopupTemplateList = '//*[@data-testid=\'dapper-scrollbars\'][.//*[contains(@data-testid, \'template--\')]]';
const fromTemplatePopupTemplateItem = '[data-testid=\'template--%ITEM%\']';
const fromTemplatePopupTemplatePanel = '[data-testid=\'template-panel\']';
const fromTemplatePopupPreviewCell = '//*[@data-testid=\'template-panel\']//div[h5[text()=\'Cells\']]/p[text()=\'%NAME%\']';

const exportPopupDismiss = '[data-testid=\'overlay--header\'] [type=\'button\'][class*=dismiss]';
const exportPopupCodeMirror = '.CodeMirror';
const exportPopupDownloadJSON = '[data-testid=\'overlay--footer\'] [data-testid=\'button\'][title=\'Download JSON\']';
const exportPopupSaveAsTemplate = '[data-testid=\'overlay--footer\'] [data-testid=\'button\'][title=\'Save as template\']';
const exportPopupCopyToClipboard = '[data-testid=\'overlay--footer\'] [data-testid=\'button-copy\']';

const urlCtx = 'dashboards';

class dashboardsPage extends influxPage {

    constructor(driver){
        super(driver);
    }

    async isLoaded(){
        await super.isLoaded([{type: 'css', selector: createDashboardDropdown},
            {type: 'css', selector: filterDashboards} ,
            {type: 'css', selector: sortTypeButton}
           // {type: 'css', selector: modifiedSortButton}
        ], urlCtx);
    }

    async getCreateDashboardDropdown(){
        return await this.driver.findElement(By.css(createDashboardDropdown));
    }

    async getFilterDashboards(){
        return await this.driver.findElement(By.css(filterDashboards));
    }

    async getSortTypeButton(){
        return await this.driver.findElement(By.css(sortTypeButton));
    }

    async getSortTypeItem(item){
        return await this.driver.findElement(By.css(sortTypeItem.replace('%ITEM%', item)));
    }

//    async getModifiedSortButton(){
//        return await this.driver.findElement(By.css(modifiedSortButton));
//    }

    async getCreateDashboardItem(item){
        return await this.driver.findElement(By.css(`[data-testid^=add-resource-dropdown--][id='${item}']`));
    }

    async getCreateDashboardDropdownEmpty(){
        return await this.driver.findElement(By.css(createDashboardDropdownEmpty));
    }

    static getCreateDashboardDropdownEmptySelector(){
        return { type: 'css', selector: createDashboardDropdownEmpty};
    }

    async getCreateDashboardItems(){
        return await this.driver.findElements(By.css(createDashboardItems));
    }

    async getDashboardCardByName(name){
        return await this.driver.findElement(By.xpath(dashboardCardByName.replace('%NAME%', name)));
    }

    static getDashboardCardSelectorByName(name){
        return {  type: 'xpath', selector: dashboardCardByName.replace('%NAME%', name) };
    }

    async getDashboardCardExportButton(name){
        return await this.driver.findElement(By.xpath(dashboardCardExportButton.replace('%NAME%', name)));
    }

    static getDashboardCardExportButtonSelector(name){
        return { type: 'xpath', selector: dashboardCardExportButton.replace('%NAME%', name)};
    }

    async getDashboardCardExportConfirm(name){
        return await this.driver.findElement(By.xpath(dashboardCardExportConfirm.replace('%NAME%', name)));
    }

    async getDashboardCardCloneButton(name){
        return await this.driver.findElement(By.xpath(dashboardCardCloneButton.replace('%NAME%', name)));
    }

    static getDashboardCardCloneButtonSelector(name){
        return { type: 'xpath', selector: dashboardCardCloneButton.replace('%NAME%', name)};
    }


    async getDashboardCardDeleteButton(name){
        return await this.driver.findElement(By.xpath(dashboardCardDeleteButton.replace('%NAME%', name)));
    }

    static getDashboardCardDeleteButtonSelector(name){
        return { type: 'xpath', selector: dashboardCardDeleteButton.replace('%NAME%', name)};
    }

    async getDashboardCardName(name){
        return await this.driver.findElement(By.xpath(dashboardCardName.replace('%NAME%', name)));
    }

    async getDashboardCardNameButton(name){
        return await this.driver.findElement(By.xpath(dashboardCardNameButton.replace('%NAME%', name)));
    }

    async getDashboardCardNameInput(name){
        return await this.driver.findElement(By.xpath(dashboardCardNameInput.replace('%NAME%', name)));
    }

    async getDashboardCardDescription(name){
        return await this.driver.findElement(By.xpath(dashboardCardDescription.replace('%NAME%', name)));
    }

    async getDashboardCardDescriptionEdit(name){
        return await this.driver.findElement(By.xpath(dashboardCardDescriptionEdit.replace('%NAME%', name)));
    }

    async getDashboardCardDescriptionInput(name){
        return await this.driver.findElement(By.xpath(dashboardCardDescriptionInput.replace('%NAME%', name)));
    }

    async getDashboardCardLabelsEmpty(name){
        return await this.driver.findElement(By.xpath(dashboardCardLabelsEmpty.replace('%NAME%', name)));
    }

    async getDashboardCardAddLabels(name){
        return await this.driver.findElement(By.xpath(dashboardCardAddLabels.replace('%NAME%', name)));
    }

    async getAddLabelsPopoverLabel(label){
        return await this.driver.findElement(By.xpath(addLabelsPopoverLabel.replace('%LABEL%', label)));
    }

    static getAddLabelsPopoverLabelSelector(label){
        return { type: 'xpath', selector: addLabelsPopoverLabel.replace('%LABEL%', label)};
    }

    async getAddLabelsPopoverFilter(){
        return await this.driver.findElement(By.css(addLabelsPopoverFilter));
    }

    async getAddLabelsLabelPills(){
        return await this.driver.findElements(By.css(addLabelsLabelPills));
    }

    async getAddLabelsPopoverListItem(item){
        return await this.driver.findElement(By.css(addLabelsPopoverListItem.replace('%ITEM%', item)));
    }

    async getAddLabelsPopoverNewItem(){
        return await this.driver.findElement(By.css(addLabelsPopoverNewItem));
    }

    static getAddLabelsPopoverNewItemSelector(){
        return { type: 'css', selector: addLabelsPopoverNewItem };
    }

    async getDashboardCardLabelPill(name, label){
        return await this.driver.findElement(By.xpath(dashboardCardLabelPill
            .replace('%NAME%', name).replace('%LABEL%', label)));
    }

    static getDashboardCardLabelPillSelector(name, label){
        return { type: 'xpath', selector: dashboardCardLabelPill
            .replace('%NAME%', name).replace('%LABEL%', label) };
    }

    async getAddLabelsPopover(){
        return await this.driver.findElement(By.css(addLabelsPopover));
    }

    static getAddLabelsPopoverSelector(){
        return { type: 'css', selector: addLabelsPopover};
    }

    async getDashboardCardLabelPillDelete(name, label){
        return await this.driver.findElement(By.xpath(dashboardCardLabelPillDelete
            .replace('%NAME%', name).replace('%LABEL%', label)));
    }

    async getImportPopupUploadFileRadio(){
        return await this.driver.findElement(By.css(importPopupUploadFileRadio));
    }

    async getImportPopupPasteJSONRadio(){
        return await this.driver.findElement(By.css(importPopupPasteJSONRadio));
    }

    async getImportPopupImportJSONButton(){
        return await this.driver.findElement(By.css(importPopupImportJSONButton));
    }

    async getImportPopupDismiss(){
        return await this.driver.findElement(By.css(importPopupDismiss));
    }

    async getImportPopupFileInput(){
        return await this.driver.findElement(By.css(importPopupFileInput));
    }

    static getImportPopupFileInputSelector(){
        return { type: 'css', selector: importPopupFileInput };
    }

    async getImportPopupFileInputHeader(){
        return await this.driver.findElement(By.css(importPopupFileInputHeader));
    }

    async getImportPopupDragNDropFile(){
        return await this.driver.findElement(By.css(importPopupDragNDropFile));
    }

    async getImportPopupJSONTextarea(){
        return await this.driver.findElement(By.css(importPopupJSONTextarea));
    }

    async getFromTemplatePopupDismiss(){
        return await this.driver.findElement(By.css(fromTemplatePopupDismiss));
    }

    async getFromTemplatePopupCancel(){
        return await this.driver.findElement(By.css(fromTemplatePopupCancel));
    }

    async getFromTemplatePopupCreateDBoard(){
        return await this.driver.findElement(By.css(fromTemplatePopupCreateDBoard));
    }

    async getFromTemplatePopupTemplateList(){
        return await this.driver.findElement(By.xpath(fromTemplatePopupTemplateList));
    }

    async getFromTemplatePopupTemplateItem(item){
        return await this.driver.findElement(By.css(fromTemplatePopupTemplateItem.replace('%ITEM%', item)));
    }

    async getFromTemplatePopupTemplatePanel(){
        return await this.driver.findElement(By.css(fromTemplatePopupTemplatePanel));
    }

    async getfromTemplatePopupPreviewCell(name){
        return await this.driver.findElement(By.xpath(fromTemplatePopupPreviewCell.replace('%NAME%', name)));
    }

    async getDashboardCardNames(){
        return await this.driver.findElements(By.css(dashboardCardNames));
    }

    async getDashboardCardDeleteConfirm(name){
        return await this.driver.findElement(By.xpath(dashboardCardDeleteConfirm.replace('%NAME%', name)));
    }

    async getDashboardCardCloneConfirm(name){
        return await this.driver.findElement(By.xpath(dashboardCardCloneConfirm.replace('%NAME%', name)));
    }

    async getExportPopupDismiss(){
        return await this.driver.findElement(By.css(exportPopupDismiss));
    }

    async getExportPopupCodeMirror(){
        return await this.driver.findElement(By.css(exportPopupCodeMirror));
    }

    async getExportPopupDownloadJSON(){
        return await this.driver.findElement(By.css(exportPopupDownloadJSON));
    }

    async getExportPopupSaveAsTemplate(){
        return await this.driver.findElement(By.css(exportPopupSaveAsTemplate));
    }

    async getexportPopupCopyToClipboard(){
        return await this.driver.findElement(By.css(exportPopupCopyToClipboard));
    }

}

module.exports = dashboardsPage;
