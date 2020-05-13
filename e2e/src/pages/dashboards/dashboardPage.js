const influxPage = require(__srcdir + '/pages/influxPage.js');
const { By } = require('selenium-webdriver');

const pageTitle = '[data-testid=page-title]';
const nameInput = '[data-testid=page-header] [data-testid=input-field]';
//const graphToolTips = '[data-testid=page-header--right] [data-testid=graphtips-question-mark]';
const graphToolTips = '[data-testid=page-control-bar] [data-testid=graphtips-question-mark]';
//const addCellButtonHeader = '//*[@data-testid=\'page-header--right\']//*[@data-testid=\'button\'][.//*[text()=\'Add Cell\']]';
const addCellButtonHeader = '//*[@data-testid=\'page-control-bar--left\']//*[@data-testid=\'button\'][.//*[text()=\'Add Cell\']]';
//const addNoteButton = '//*[@data-testid=\'page-header--right\']//*[@data-testid=\'button\'][.//*[text()=\'Add Note\']]';
const addNoteButton = '//*[@data-testid=\'page-control-bar--left\']//*[@data-testid=\'button\'][.//*[text()=\'Add Note\']]';
//const variablesButton = '//*[@data-testid=\'page-header--right\']//*[@data-testid=\'button\'][.//*[text()=\'Variables\']]';
const variablesButton = '//*[@data-testid=\'page-control-bar--left\']//*[@data-testid=\'button\'][.//*[text()=\'Variables\']]';
//const timeLocaleDropdown = '//*[@data-testid=\'page-header--right\']//*[@data-testid=\'dropdown--button\'][.//*[contains(@class,\'annotate\')]]';
const timeLocaleDropdown = '//*[@data-testid=\'page-control-bar--right\']//*[@data-testid=\'dropdown--button\'][.//*[contains(@class,\'annotate\')]]';
//const autorefresh = '//*[@data-testid=\'page-header--right\']/*[contains(@class,\'autorefresh-dropdown\')]';
const autorefresh = '//*[@data-testid=\'page-control-bar--right\']/*[contains(@class,\'autorefresh-dropdown\')]';
//const refreshRateDropdown = '//*[@data-testid=\'page-header--right\']//*[contains(@class,\'autorefresh\')]//*[@data-testid=\'dropdown--button\']';
const refreshRateDropdown = '//*[@data-testid=\'page-control-bar--right\']//*[contains(@class,\'autorefresh\')]//*[@data-testid=\'dropdown--button\']';
//const forceRefreshButton = '//*[@data-testid=\'page-header--right\']//*[contains(@class,\'autorefresh\')]//*[@data-testid=\'square-button\']';
const forceRefreshButton = '//*[@data-testid=\'page-control-bar--right\']//*[contains(@class,\'autorefresh\')]//*[@data-testid=\'square-button\']';
//const timeRangeDropdown = '//*[@data-testid=\'page-header--right\']//*[@data-testid=\'timerange-dropdown\']//*[@data-testid=\'dropdown--button\']';
const timeRangeDropdown = '//*[@data-testid=\'page-control-bar--right\']//*[@data-testid=\'timerange-dropdown\']//*[@data-testid=\'dropdown--button\']';
const timeRangeDropdownSelected = '[data-testid=\'timerange-dropdown\'] [class*=selected]';
const timeRangeDropdownItem = '[data-testid=dropdown-item-past%ITEM%]';
//const presentationModeButton = '//*[@data-testid=\'page-header--right\']//*[contains(@title,\'Presentation\')]';
const presentationModeButton = '[data-testid=\'presentation-mode-toggle\']';
const variableValueDropdownButtonForVar = '//*[@class=\'variable-dropdown\'][.//*[text()=\'%VARNAME%\']]//*[@data-testid=\'variable-dropdown--button\']';
const variableValueDropdownItem = '//*[@class=\'variable-dropdown\'][.//*[text()=\'%VARNAME%\']]//*[@data-testid=\'variable-dropdown\']//*[@data-testid=\'variable-dropdown--item\'][.//*[text()=\'%ITEM%\']]'

const dropdownMenuItem = '//*[@data-testid=\'dropdown-menu\']//*[contains(@data-testid,\'dropdown-item\')]/*[text()=\'%ITEM%\']';
const dropdownMenuDivider = '//*[@data-testid=\'dropdown-menu\']//*[@data-testid=\'dropdown-divider\'][text()=\'%LABEL%\']';

const emptyStateTextLink = '[data-testid=\'empty-state--text\'] a';
const emptyStateAddCellButton = '[data-testid=\'empty-state\'] [data-testid=\'add-cell--button\']';

const cellByName = '//*[contains(@class, \' cell \')][.//*[text()=\'%NAME%\']]';
const cellEmptyGraphMessage = '//*[contains(@class, \' cell \')][.//*[text()=\'%NAME%\']]//*[@data-testid=\'empty-graph--no-queries\']';
const cellEmptyGraphNoResults = '//*[contains(@class, \' cell \')][.//*[text()=\'%NAME%\']]//*[@data-testid=\'empty-graph--no-results\']';
const cellEmptyGraphError = '//*[contains(@class, \' cell \')][.//*[text()=\'%NAME%\']]//*[@data-testid=\'empty-graph--error\']';
const cellEmptyGraphErrorIcon = '//*[contains(@class, \' cell \')][.//*[text()=\'%NAME%\']]//*[@data-testid=\'empty-graph--error\']//*[contains(@class,\'empty-graph-error--icon\')]';
const emptyGraphPopoverContents = '[data-testid^=emptygraph-popover--contents]';
const cellTitle = '//*[@class=\'cell--header\'][./*[text()=\'%NAME%\']]';
const cellHandleByName = '//*[contains(@class, \' cell \')][.//*[text()=\'%NAME%\']]//*[@class=\'cell--draggable\']';
const cellResizerByName = '//*[contains(@class, \' cell \')][.//*[text()=\'%NAME%\']]//*[@class=\'react-resizable-handle\']';
const cellContextToggleByName = '//*[contains(@class, \' cell \')][.//*[text()=\'%NAME%\']]//*[@data-testid=\'cell-context--toggle\']';
const cellNoteByName = '//*[contains(@class, \' cell \')][.//*[text()=\'%NAME%\']]//*[@class=\'cell--note-indicator\']';
const cellPopoverContents = '[data-testid=popover--contents]';
const cellPopoverContentsConfigure = '[data-testid=popover--contents] [data-testid=\'cell-context--configure\']';
const cellPopoverContentsAddNote = '[data-testid=popover--contents] [data-testid=\'cell-context--note\']';
const cellPopoverContentsClone = '[data-testid=popover--contents] [data-testid=\'cell-context--clone\']';
const cellPopoverContentsDelete = '[data-testid=popover--contents] [data-testid=\'cell-context--delete\']';
const cellPopoverContentsDeleteConfirm = '[data-testid=popover--contents] [data-testid=\'cell-context--delete-confirm\']';
const cellPopoverContentsEditNote = '[data-testid=cell-context--note]';
const cellCanvasLine = '//*[contains(@class, \' cell \')][.//*[text()=\'%NAME%\']]//*[@data-testid=\'giraffe-layer-line\']';
const cellCanvasAxes = '//*[contains(@class, \' cell \')][.//*[text()=\'%NAME%\']]//*[@data-testid=\'giraffe-axes\']';

const cellHoverBox = '[data-testid=giraffe-layer-hover-line]';


const notePopupCodeMirror = '[data-testid=overlay--body] .CodeMirror';
const notePopupNoDataToggle = '[data-testid=overlay--body] [data-testid=slide-toggle]';
const notePopupEditorPreview = '[data-testid=overlay--body] .note-editor--preview';
const notePopupEditorPreviewTag = '[data-testid=overlay--body] .note-editor--preview %TAG%';
const notePopupEditorPreviewText = '[data-testid=overlay--body] .note-editor--preview .markdown-format';
const notePopupGuideLink = '[href*=\'markdownguide.org\']';

const notePopover = '[data-testid=popover--dialog]';
const notePopoverContents = '[data-testid=popover--dialog] .markdown-format';

const noteCellMarkdownTag = '[data-testid=cell--view-empty]  .markdown-format %TAG%';
const noteCellContextEdit = '[data-testid=cell-context--note]';

const urlCtx = 'dashboards';

class dashboardPage extends influxPage {

    constructor(driver) {
        super(driver);
    }

    async isLoaded(){
        await super.isLoaded([{type: 'css', selector: pageTitle},
            {type: 'css', selector: graphToolTips},
            {type: 'xpath', selector: addCellButtonHeader},
            {type: 'xpath', selector: addNoteButton},
            {type: 'xpath', selector: variablesButton},
            {type: 'xpath', selector: timeLocaleDropdown},
            {type: 'xpath', selector: autorefresh},
            {type: 'xpath', selector: timeRangeDropdown},
            {type: 'css', selector: presentationModeButton}], urlCtx);
    }


    async getPageTitle(){
        //return await this.driver.findElement(By.css(pageTitle));
        return await this.smartGetElement({type: 'css', selector: pageTitle});
    }

    async getNameInput(){
        return await this.driver.findElement(By.css(nameInput));
    }

    async getCellByName(name){
        return await this.driver.findElement(By.xpath(cellByName.replace('%NAME%', name)));
    }

    static getCellSelectorByName(name){
        return {type: 'xpath', selector: cellByName.replace('%NAME%', name) };
    }

    async getCellsByName(name){
        return await this.driver.findElements(By.xpath(cellByName.replace('%NAME%', name)));
    }

    async getGraphToolTips(){
        return await this.driver.findElement(By.css(graphToolTips));
    }

    async getAddCellButtonHeader(){
        return await this.driver.findElement(By.xpath(addCellButtonHeader));
    }

    async getAddNoteButton(){
        return await this.driver.findElement(By.xpath(addNoteButton));
    }

    async getVariablesButton(){
        return await this.driver.findElement(By.xpath(variablesButton));
    }

    async getTimeLocaleDropdown(){
        return await this.driver.findElement(By.xpath(timeLocaleDropdown));
    }

    async getRefreshRateDropdown(){
        return await this.driver.findElement(By.xpath(refreshRateDropdown));
    }

    async getForceRefreshButton(){
        return await this.driver.findElement(By.xpath(forceRefreshButton));
    }

    async getTimeRangeDropdown(){
        return await this.driver.findElement(By.xpath(timeRangeDropdown));
    }

    async getTimeRangeDropdownSelected(){
        return await this.driver.findElement(By.css(timeRangeDropdownSelected));
    }

    async getTimeRangeDropdownItem(item){
        return await this.driver.findElement(By.css(timeRangeDropdownItem.replace('%ITEM%', item)));
    }

    async getPresentationModeButton(){
        return await this.driver.findElement(By.css(presentationModeButton));
    }

    async getEmptyStateTextLink(){
        return await this.driver.findElement(By.css(emptyStateTextLink));
    }

    async getEmptyStateAddCellButton(){
        return await this.driver.findElement(By.css(emptyStateAddCellButton));
    }

    async getDropdownMenuItem(item){
        return await this.driver.findElement(By.xpath(dropdownMenuItem.replace('%ITEM%', item)));
    }

    async getdropdownMenuDivider(label){
        return await this.driver.findElement(By.xpath(dropdownMenuDivider.replace('%LABEL%', label)));
    }

    async getCellEmptyGraphMessage(name){
        return await this.driver.findElement(By.xpath(cellEmptyGraphMessage.replace('%NAME%', name)));
    }

    async getCellEmptyGraphNoResults(name){
        return await this.driver.findElement(By.xpath(cellEmptyGraphNoResults.replace('%NAME%', name)));
    }

    async getCellEmptyGraphError(name){
        return await this.driver.findElement(By.xpath(cellEmptyGraphError.replace('%NAME%', name)));
    }

    async getCellTitle(name){
        return await this.driver.findElement(By.xpath(cellTitle.replace('%NAME%', name)));
    }

    async getCellHandleByName(name){
        return await this.driver.findElement(By.xpath(cellHandleByName.replace('%NAME%', name)));
    }

    async getCellResizerByName(name){
        return await this.driver.findElement(By.xpath(cellResizerByName.replace('%NAME%', name)));
    }

    static getCellPopoverContentsSelector(){
        return { type: 'css', selector: cellPopoverContents};
    }

    async getCellContextToggleByName(name){
        return await this.driver.findElement(By.xpath(cellContextToggleByName.replace('%NAME%', name)));
    }

    async getCellPopoverContentsConfigure(){
        return await this.driver.findElement(By.css(cellPopoverContentsConfigure));
    }

    async getCellPopoverContentsAddNote(){
        return await this.driver.findElement(By.css(cellPopoverContentsAddNote));
    }

    async getCellPopoverContentsClone(){
        return await this.driver.findElement(By.css(cellPopoverContentsClone));
    }
    
    async getCellPopoverContentsDelete(){
        return await this.driver.findElement(By.css(cellPopoverContentsDelete));
    }

    async getCellPopoverContentsDeleteConfirm(){
        return await this.driver.findElement(By.css(cellPopoverContentsDeleteConfirm));
    }

    async getcellPopoverContentsEditNote(){
        return await this.driver.findElement(By.css(cellPopoverContentsEditNote));
    }

    async getNotePopupCodeMirror(){
        return await this.driver.findElement(By.css(notePopupCodeMirror));
    }

    async getNotePopupNoDataToggle(){
        return await this.driver.findElement(By.css(notePopupNoDataToggle));
    }

    async getNotePopupEditorPreview(){
        return await this.driver.findElement(By.css(notePopupEditorPreview));
    }

    async getNotePopupEditorPreviewText(){
        return await this.driver.findElement(By.css(notePopupEditorPreviewText));
    }

    async getNotePopupGuideLink(){
        return await this.driver.findElement(By.css(notePopupGuideLink));
    }

    async getCellNoteByName(name){
        return await this.driver.findElement(By.xpath(cellNoteByName.replace('%NAME%', name)));
    }

    async getNotePopoverContents(){
        return await this.driver.findElement(By.css(notePopoverContents));
    }

    async getNotePopover(){
        return await this.driver.findElement(By.css(notePopover));
    }

    static getNotePopoverSelector(){
        return { type: 'css', selector: notePopover};
    }

    async getNotePopupEditorPreviewTag(tag){
        return await this.driver.findElement(By.css(notePopupEditorPreviewTag.replace('%TAG%', tag)));
    }

    async getCellCanvasLine(name){
        return await this.driver.findElement(By.xpath(cellCanvasLine.replace('%NAME%', name)));
    }

    async getCellCanvasAxes(name){
        return await this.driver.findElement(By.xpath(cellCanvasAxes.replace('%NAME%', name)));
    }

    async getCellHoverBox(){
        return await this.driver.findElement(By.css(cellHoverBox));
    }

    async getEmptyGraphPopoverContents(){
        return await this.driver.findElement(By.css(emptyGraphPopoverContents));
    }

    async getCellEmptyGraphErrorIcon(name){
        return await this.driver.findElement(By.xpath(cellEmptyGraphErrorIcon.replace('%NAME%', name)));
    }

    async getVariableValueDropdownButtonForVar(varname){
        return await this.driver.findElement(By.xpath(variableValueDropdownButtonForVar.replace('%VARNAME%', varname)));
    }

    static getVariableValueDropdownButtonForVarSelector(varname){
        return {type: 'xpath', selector: variableValueDropdownButtonForVar.replace('%VARNAME%', varname)};
    }

    async getVariableValueDropdownItem(varname,item){
        return await this.driver.findElement(By.xpath(variableValueDropdownItem
            .replace("%VARNAME%", varname)
            .replace("%ITEM%", item)));
    }

    async getNoteCellMarkdownTag(tag){
        return await this.driver.findElement(By.css(noteCellMarkdownTag.replace('%TAG%', tag)));
    }
}

module.exports = dashboardPage;
