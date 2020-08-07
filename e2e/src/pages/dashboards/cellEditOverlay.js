const influxPage = require(__srcdir + '/pages/influxPage.js');
const { By } = require('selenium-webdriver');

const timeMachineOverlay = '[data-testid=overlay]';
//const cellTitle = '[data-testid=overlay] [data-testid=page-header--left] [data-testid=page-title]';
const cellTitle = '[data-testid=overlay] [data-testid=page-header] [data-testid=page-title]';
const cellNameInput = ' [data-testid=renamable-page-title--input]';
//const viewTypeDropdown = '[data-testid=overlay] [data-testid=page-header--right] [data-testid=\'view-type--dropdown\']';
const viewTypeDropdown = '[data-testid=overlay] [data-testid=page-control-bar--left] [data-testid=\'view-type--dropdown\']';
const viewTypeListContents = '[data-testid=\'view-type--dropdown\'] [data-testid=dropdown-menu--contents]';
const viewTypeItem = '[data-testid=\'view-type--%ITEM%\']';
//const customizeButton = '[data-testid=overlay] [data-testid=page-header--right] [data-testid=\'cog-cell--button\']';
const customizeButton = '[data-testid=overlay] [data-testid=page-control-bar--left] [data-testid=\'cog-cell--button\']';
//const editCancel = '[data-testid=overlay] [data-testid=page-header--right] [data-testid=\'cancel-cell-edit--button\']';
const editCancel = '[data-testid=overlay] [data-testid=page-control-bar--right] [data-testid=\'cancel-cell-edit--button\']';
//const saveCell = '[data-testid=overlay] [data-testid=page-header--right] [data-testid=\'save-cell--button\']';
const saveCell = '[data-testid=overlay] [data-testid=page-control-bar--right] [data-testid=\'save-cell--button\']';
const TMTop = '.time-machine--top';
const TMBottom = '[data-testid=\'time-machine--bottom\']';
const TMViewEmptyGraphQueries = '[data-testid=overlay] [data-testid=empty-graph--no-queries]';
const TMViewNoResults = '[data-testid=overlay] [data-testid=empty-graph--no-results]';
const TMResizerHandle = '[data-testid=overlay] [data-testid=draggable-resizer--handle]';
const viewRawDataToggle = '[data-testid=overlay] .view-raw-data-toggle';
const TMAutorefreshDropdown = '[data-testid=overlay] [data-testid=time-machine--bottom] .autorefresh-dropdown [data-testid=dropdown--button]';
const TMAutorefreshDropdownPaused = '[data-testid=overlay] [data-testid=time-machine--bottom] .autorefresh-dropdown [data-testid=dropdown--button] [data-testid=\'icon\'].pause';
const TMAutorefreshDropdownSelected = '[data-testid=overlay] [data-testid=time-machine--bottom] .autorefresh-dropdown [data-testid=dropdown--button] .cf-dropdown--selected';
const TMAutorefreshItem = '//*[@data-testid=\'dropdown-item\'][./*[text()=\'%ITEM%\']]';
const TMAutorefreshForceButton = '[class=time-machine] [class^=autorefresh-dropdown] [data-testid=square-button]';
const TMTimeRangeDropdown = '[data-testid=overlay] [data-testid=time-machine--bottom] [data-testid=timerange-dropdown]';
const TMTimeRangeDropdownItem = '[data-testid=dropdown-item-%ITEM%]';
const TMTimeRangeDropdownContents = '[data-testid=dropdown-menu--contents]';
const switchToScriptEditor = '[data-testid=overlay] [data-testid=time-machine--bottom] [data-testid=switch-to-script-editor] ';
const TMSwitchToQueryBuilder = '[data-testid=switch-query-builder-confirm--button]';
const TMSwitchToQBuilderConfirm = '[data-testid=switch-query-builder-confirm--confirm-button]';
const TMSwitchToQBuilderWarn = '[data-testid=switch-query-builder-confirm--popover--contents]';
const timemachineSubmit = '[data-testid=time-machine-submit-button] ';
const TMQueryTabByName = '//*[contains(@class,\'query-tab \')][./*[@title=\'%NAME%\']]';
const TMQueryBuilder = '[data-testid=query-builder]';
//const functionSelect = '[data-testid=function-selector]';
//const bucketSelect = '[data-testid=bucket-selector]';
const TMBucketSelectorBucket = '[data-testid=bucket-selector] [data-testid=\'selector-list %NAME%\']';
const TMBucketSelectorFilter = '[data-testid=bucket-selector] [data-testid=\'input-field\']';
const TMBuilderCards = '[data-testid=builder-card]';
const bucketSelectItem = '[data-testid=bucket-selector] [data-testid=\'selector-list %ITEM%\'] ';
const bucketSelectSearch = '[data-testid=bucket-selector] [data-testid=builder-card--menu] [class *= search]';
const scriptEditorCodeMirror = '.CodeMirror';
//const scriptMonacoEditor = '.monaco-editor';
const scriptMonacoEditor = '.inputarea';
const TMFluxEditor = '[data-testid=flux-editor]';
const graphCanvas = '[data-testid=\'overlay\'] canvas[data-testid^=giraffe-layer]';
const graphCanvasAxes = '[data-testid=\'overlay\'] canvas[data-testid=giraffe-axes]';
const viewOptionsContainer = '.view-options';
const TMEmptyGraphErrMessage = '.empty-graph-error pre';
const TMDownloadCSV = '[data-testid=button][title*=\'CSV\']';

//Query builder
const TMQBSelectedBucket = '[data-testid=bucket-selector] [data-testid^=\'selector-list\'][class*=selected]';
const TMQBSelectedTagOfCard = '//*[@data-testid=\'builder-card\'][.//*[@data-testid=\'tag-selector--container %INDEX%\']]//*[contains(@data-testid,\'selector-list\')][contains(@class,\'selected\')]';
const TMBuilderCardMenuDurationInput = '[data-testid=\'duration-input\']';
const TMBuilderCardMenuFunctionListItem = '[data-testid=\'selector-list %ITEM%\']';
const TMBuilderCardMenuFunctionFilter = '[data-testid=\'input-field\'][placeholder*=\'functions\']';
const TMBuilderCardMenuFunctionListItems = '[data-testid=function-selector] [data-testid^=\'selector-list\']';
const TMQBSelectedFunctionsByName = '[data-testid=\'selector-list %NAME%\'].selected';
const TMQBDurationSuggestions = '[data-testid=\'dropdown-menu--contents\'] [data-testid=\'dropdown-item\']';
const TMQBDurationSuggestionByName = '//*[@data-testid=\'dropdown-menu--contents\']//*[@data-testid=\'dropdown-item\'][./*[text()=\'%NAME%\']]';
const TMBuilderTabsAddQuery = '[data-testid=overlay] [class=time-machine-queries--tabs] [data-testid=square-button]';
const TMQBActiveQueryTab = '.query-tab__active';
const TMQBQueryTabByName = '//*[contains(@class,\'query-tab\')][./*[text()=\'%NAME%\']]';
const TMQBRightClickItem = '[data-testid=\'right-click--%ITEM%-tab\']';
const TMQBQueryTabNameInput = 'div.cf-input__focused input';
const TMQBQueryTabs = '.time-machine-queries .query-tab';
const TMQBCustomFunctionGroup = '[data-testid=custom-function]';
const TMQBCustomWindowPeriod = '[data-testid=\'custom-window-period\']';
const TMQBAutoWindowPeriod = '[data-testid=\'auto-window-period\']';

//Query Editor
const TMQEFunctionCategory = '//*[@class=\'flux-toolbar--heading\'][text()=\'%NAME%\']';
const TMQEFunctionListItem = '[data-testid=\'flux--%NAME%\']';
const TMQEFunctionListItemInjector = '[data-testid=\'flux--%NAME%--inject\']';
const TMQEFunctionFilter = '.flux-toolbar--search [data-testid=\'input-field\']';
const TMQEFunctionPopup = '[data-testid=\'toolbar-popover--contents\']';
const TMQEFunctionPopupDescription = '[data-testid=\'toolbar-popover--contents\'] .flux-functions-toolbar--description span';
const TMQEFunctionPopupSnippet = '//*[@data-testid=\'toolbar-popover--contents\']//*[@class=\'flux-function-docs--heading\'][text()=\'Example\']/../*[@class=\'flux-function-docs--snippet\']';
const TMQEVariablesTab = '[data-testid=toolbar-tab][title=\'Variables\']';
const TMQEVariablesLabel = '[data-testid=\'variable-name--%LABEL%\']';
const TMQEVariablesInject = '[data-testid=\'variable--%VAR%--inject\']';
const TMQEVariablesFilter = '[data-testid=input-field][placeholder^=\'Filter Variables\']';
const TMQEVariablesPopover = '[data-testid=toolbar-popover--dialog]';
const TMQEVariablesPopoverContents = '[data-testid=\'toolbar-popover--contents\']';
const TMQEVariablePopoverDropdown = '[data-testid=\'variable--tooltip-dropdown\'] [data-testid=\'dropdown--button\']  .caret-down';

//RawData
const TMRawDataTable = '[data-testid=raw-data-table]';
const TMRawDataToggle = '[data-testid=raw-data--toggle]';
const TMRawDataReactGrid = '[data-testid=raw-data-table] [class=ReactVirtualized__Grid__innerScrollContainer]';
const TMRawDataCells = '[class=\'raw-flux-data-table--cell\']';
const TMRawDataCellByIndex = '[data-testid=raw-data-table] .raw-flux-data-table--cell:nth-of-type(%INDEX%)';
const TMRawDataScrollTrackH = '[data-testid=raw-data-table] [class=fancy-scroll--track-h]';
const TMRawDataScrollHThumb = '[data-testid=raw-data-table] [class=fancy-scroll--thumb-h]';
const TMRawDataScrollTrackV = '[data-testid=raw-data-table] [class=fancy-scroll--track-v]';
const TMRawDataScrollVThumb = '[data-testid=raw-data-table] [class^=fancy-scroll--thumb-v]';

const urlCtx = 'cells';

class cellEditOverlay extends influxPage {

    constructor(driver) {
        super(driver);
    }

    async isLoaded(){
        await super.isLoaded([{type: 'css', selector: cellTitle},
            {type: 'css', selector: viewTypeDropdown},
            {type: 'css', selector: customizeButton},
            {type: 'css', selector: editCancel},
            {type: 'css', selector: saveCell},
            {type: 'css', selector: TMResizerHandle},
            {type: 'css', selector: viewRawDataToggle},
            {type: 'css', selector: TMAutorefreshDropdown},
            {type: 'css', selector: TMTimeRangeDropdown},
            // {type: 'css', selector: switchToScriptEditor},
            {type: 'css', selector: timemachineSubmit},
            //{type: 'css', selector: queryBuilder},
            // {type: 'css', selector: functionSelect},
            //{type: 'css', selector: bucketSelect}
        ], urlCtx);
    }

    static getTimeMachineOverlay(){
        return {type: 'css', selector: timeMachineOverlay};
    }

    async getTMTop(){
        return await this.driver.findElement(By.css(TMTop));
    }

    async getTMBottom(){
        return await this.driver.findElement(By.css(TMBottom));
    }

    async getCellTitle(){
        return await this.driver.findElement(By.css(cellTitle));
    }

    async getCellNameInput(){
        return await this.driver.findElement(By.css(cellNameInput));
    }

    async getBucketSelectItem(item){
        return await this.driver.findElement(By.css(bucketSelectItem.replace('%ITEM%', item)));
    }

    async getBucketSelectSearch(){
        return await this.driver.findElement(By.css(bucketSelectSearch));
    }

    static getBucketSelectSearchSelector(){
        return { type: 'css', selector: bucketSelectSearch };
    }

    async getEditCancel(){
        return await this.driver.findElement(By.css(editCancel));
    }

    async getSaveCell(){
        return await this.driver.findElement(By.css(saveCell));
    }

    async getTMTimeRangeDropdown(){
        return await this.driver.findElement(By.css(TMTimeRangeDropdown));
    }

    async getTMTimeRangeDropdownItem(item){
        //console.log("DEBUG selector " + TMTimeRangeDropdownItem.replace('%ITEM%', item));
        return await this.driver.findElement(By.css(TMTimeRangeDropdownItem.replace('%ITEM%', item)));
    }

    async getTimemachineSubmit(){
        return await this.driver.findElement(By.css(timemachineSubmit));
    }

    async getSwitchToScriptEditor(){
        return await this.driver.findElement(By.css(switchToScriptEditor));
    }

    async getScriptEditorCodeMirror(){
        return await this.driver.findElement(By.css(scriptEditorCodeMirror));
    }

    async getScriptMonacoEditor(){
        return await this.driver.findElement(By.css(scriptMonacoEditor));
    }

    async getGraphCanvas(){
        return await this.driver.findElement(By.css(graphCanvas));
    }

    static getGraphCanvasSelector(){
        return { type: 'css', selector: graphCanvas };
    }

    async getGraphCanvasAxes(){
        return await this.driver.findElement(By.css(graphCanvasAxes));
    }

    static getGraphCanvasAxesSelector(){
        return { type: 'css', selector: graphCanvasAxes };
    }

    async getViewTypeDropdown(){
        return await this.driver.findElement(By.css(viewTypeDropdown));
    }

    async getViewTypeItem(item){
        return await this.driver.findElement(By.css(viewTypeItem.replace('%ITEM%', item)));
    }

    async getViewTypeListContents(){
        return await this.driver.findElement(By.css(viewTypeListContents));
    }

    static getViewTypeListContentsSelector(){
        return { type: 'css', selector: viewTypeListContents };
    }

    async getCustomizeButton(){
        return await this.driver.findElement(By.css(customizeButton));
    }

    async getViewOptionsContainer(){
        return await this.driver.findElement(By.css(viewOptionsContainer));
    }

    static getViewOptionsContainerSelector(){
        return { type: 'css', selector: viewOptionsContainer };
    }

    async getTMViewEmptyGraphQueries(){
        return await this.driver.findElement(By.css(TMViewEmptyGraphQueries));
    }

    async getTMViewNoResults(){
        return await this.driver.findElement(By.css(TMViewNoResults));
    }

    async getTMAutorefreshDropdown(){
        return await this.driver.findElement(By.css(TMAutorefreshDropdown));
    }

    async getTMAutorefreshDropdownSelected(){
        return await this.driver.findElement(By.css(TMAutorefreshDropdownSelected))
    }

    async getTMAutorefreshDropdownPaused(){
        return await this.driver.findElement(By.css(TMAutorefreshDropdownPaused));
    }

    async getTMAutorefreshItem(item){
        return await this.driver.findElement(By.xpath(TMAutorefreshItem.replace('%ITEM%', item)));
    }

    async getTMAutorefreshForceButton(){
        return await this.driver.findElement(By.css(TMAutorefreshForceButton));
    }

    static getTMAutorefreshForceButtonSelector(){
        return { type: 'css', selector: TMAutorefreshForceButton };
    }

    async getTMTimeRangeDropdownContents(){
        return await this.driver.findElement(By.css(TMTimeRangeDropdownContents));
    }

    static getTMTimeRangeDropdownContentsSelector(){
        return { type: 'css', selector: TMTimeRangeDropdownContents };
    }

    async getTMQueryTabByName(name){
        return await this.driver.findElement(By.xpath(TMQueryTabByName.replace('%NAME%',name)));
    }

    async getTMQueryBuilder(){
        return await this.driver.findElement(By.css(TMQueryBuilder));
    }

    async getTMFluxEditor(){
        return await this.driver.findElement(By.css(TMFluxEditor));
    }

    async getTMDownloadCSV(){
        return await this.driver.findElement(By.css(TMDownloadCSV));
    }

    static getTMFluxEditorSelector(){
        return { type: 'css', selector: TMFluxEditor };
    }

    async getTMSwitchToQueryBuilder(){
        return await this.driver.findElement(By.css(TMSwitchToQueryBuilder));
    }

    async getTMSwitchToQBuilderConfirm(){
        return await this.driver.findElement(By.css(TMSwitchToQBuilderConfirm));
    }

    static getTMSwitchToQBuilderWarnSelector(){
        return { type: 'css', selector: TMSwitchToQBuilderWarn };
    }

    async getTMSwitchToQBuilderWarn(){
        return await this.driver.findElement(By.css(TMSwitchToQBuilderWarn));
    }

    async getTMBucketSelectorBucket(name){
        return await this.driver.findElement(By.css(TMBucketSelectorBucket.replace('%NAME%', name)));
    }

    static getTMBucketSelectorBucketSelector(name){
        return { type: 'css', selector: TMBucketSelectorBucket.replace('%NAME%', name) };
    }

    async getTMBucketSelectorFilter(){
        return await this.driver.findElement(By.css(TMBucketSelectorFilter));
    }

    async getTMBuilderCards(){
        return await this.driver.findElements(By.css(TMBuilderCards));
    }

    async getTMBuilderCardByIndex(index){
        return (await this.driver.findElements(By.css(TMBuilderCards)))[index - 1];
    }

    async getTMBuilderCardMenuDurationInput(){
        return await this.driver.findElement(By.css(TMBuilderCardMenuDurationInput));
    }

    async getTMBuilderCardMenuFunctionListItem(item){
        return await this.driver.findElement(By.css(TMBuilderCardMenuFunctionListItem.replace('%ITEM%', item)));
    }

    static getTMBuilderCardMenuFunctionListItemSelector(item){
        return { type: 'css', selector: TMBuilderCardMenuFunctionListItem.replace('%ITEM%', item) };
    }

    async getTMBuilderCardMenuFunctionFilter(){
        return await this.driver.findElement(By.css(TMBuilderCardMenuFunctionFilter));
    }

    async getTMBuilderCardMenuFunctionListItems(){
        return await this.driver.findElements(By.css(TMBuilderCardMenuFunctionListItems));
    }

    async getTMQBCustomFunctionGroup(){
        return await this.driver.findElement(By.css(TMQBCustomFunctionGroup));
    }

    async getTMQBCustomWindowPeriod(){
        return await this.driver.findElement(By.css(TMQBCustomWindowPeriod));
    }

    async getTMQBAutoWindowPeriod(){
        return await this.driver.findElement(By.css(TMQBAutoWindowPeriod));
    }

    async getTMQBDurationSuggestions(){
        return await this.driver.findElements(By.css(TMQBDurationSuggestions));
    }

    async getTMQBDurationSuggestionByName(name){
        return await this.driver.findElement(By.xpath(TMQBDurationSuggestionByName.replace('%NAME%', name)));
    }

    async getTMResizerHandle(){
        return await this.driver.findElement(By.css(TMResizerHandle));
    }

    async getTMBuilderTabsAddQuery(){
        return await this.driver.findElement(By.css(TMBuilderTabsAddQuery));
    }

    async getTMQBSelectedBucket(){
        return await this.driver.findElement(By.css(TMQBSelectedBucket));
    }

    async getTMQBSelectedTagOfCard(index){
        return await this.driver.findElement(By.xpath(TMQBSelectedTagOfCard.replace('%INDEX%', index)));
    }

    async getTMQBActiveQueryTab(){
        return await this.driver.findElement(By.css(TMQBActiveQueryTab));
    }

    async getTMQBQueryTabByName(name){
        return await this.driver.findElement(By.xpath(TMQBQueryTabByName.replace('%NAME%', name)));
    }

    static getTMQBQueryTabSelectorByName(name){
        return { type: 'xpath', selector: TMQBQueryTabByName.replace('%NAME%', name)};
    }

    async getTMQBSelectedFunctionByName(name){
        return await this.driver.findElements(By.css(TMQBSelectedFunctionsByName.replace('%NAME%', name)));
    }

    async getTMQBRightClickItem(item){
        return await this.driver.findElement(By.css(TMQBRightClickItem.replace('%ITEM%', item.toLowerCase().trim())));
    }

    async getTMQBQueryTabNameInput(){
        return await this.driver.findElement(By.css(TMQBQueryTabNameInput));
    }

    async getTMQBQueryTabs(){
        return await this.driver.findElements(By.css(TMQBQueryTabs));
    }

    async getTMEmptyGraphErrMessage(){
        return await this.driver.findElement(By.css(TMEmptyGraphErrMessage));
    }

    async getTMQEFunctionCategory(name){
        return await this.driver.findElement(By.xpath(TMQEFunctionCategory.replace('%NAME%', name)));
    }

    async getTMQEFunctionListItem(name){
        return await this.driver.findElement(By.css(TMQEFunctionListItem.replace('%NAME%', name)));
    }

    static getTMQEFunctionListItemSelector(name){
        return { type: 'css', selector: TMQEFunctionListItem.replace('%NAME%', name) };
    }

    async getTMQEFunctionListItemInjector(name){
        return await this.driver.findElement(By.css(TMQEFunctionListItemInjector.replace('%NAME%',name)));
    }

    async getTMQEFunctionFilter(){
        return await this.driver.findElement(By.css(TMQEFunctionFilter));
    }

    async getTMQEFunctionPopupDescription(){
        return await this.driver.findElement(By.css(TMQEFunctionPopupDescription));
    }

    async getTMQEFunctionPopupSnippet(){
        return await this.driver.findElement(By.xpath(TMQEFunctionPopupSnippet));
    }

    static getTMQEFunctionPopupSelector(){
        return { type: 'css', selector: TMQEFunctionPopup };
    }

    async getTMRawDataTable(){
        return await this.driver.findElement(By.css(TMRawDataTable));
    }

    static getTMRawDataTableSelector(){
        return {  type: 'css', selector: TMRawDataTable}
    }

    async getTMRawDataToggle(){
        return await this.driver.findElement(By.css(TMRawDataToggle));
    }

    async getTMRawDataCells(){
        return await this.driver.findElements(By.css(TMRawDataCells));
    }

    async getTMRawDataReactGrid(){
        return await this.driver.findElement(By.css(TMRawDataReactGrid));
    }

    async getTMRawDataScrollTrackH(){
        return await this.driver.findElement(By.css(TMRawDataScrollTrackH));
    }

    async getTMRawDataScrollHThumb(){
        return await this.driver.findElement(By.css(TMRawDataScrollHThumb));
    }

    async getTMRawDataCellByIndex(index){
        return await this.driver.findElement(By.css(TMRawDataCellByIndex.replace('%INDEX%',index)));
    }

    async getTMRawDataScrollTrackV(){
        return await this.driver.findElement(By.css(TMRawDataScrollTrackV));
    }

    async getTMRawDataScrollVThumb(){
        return await this.driver.findElement(By.css(TMRawDataScrollVThumb));
    }

    async getTMQEVariablesTab(){
        return await this.driver.findElement(By.css(TMQEVariablesTab));
    }

    async getTMQEVariablesLabel(label){
        return await this.driver.findElement(By.css(TMQEVariablesLabel.replace('%LABEL%', label)));
    }

    static getTMQEVariablesLabelSelector(label){
        return { type: 'css', selector: TMQEVariablesLabel.replace('%LABEL%', label) }
    }

    async getTMQEVariablesInject(variable){
        return await this.driver.findElement(By.css(TMQEVariablesInject.replace('%VAR%', variable)));
    }

    async getTMQEVariablesFilter(){
        return await this.driver.findElement(By.css(TMQEVariablesFilter));
    }

    async getTMQEVariablesPopover(){
        return await this.driver.findElement(By.css(TMQEVariablesPopover));
    }

    static getTMQEVariablesPopoverSelector(){
        return { type: 'css', selector: TMQEVariablesPopover };
    }

    async getTMQEVariablePopoverDropdown(){
        return await this.driver.findElement(By.css(TMQEVariablePopoverDropdown));
    }

    async getTMQEVariablesPopoverContents(){
        return await this.driver.findElement(By.css(TMQEVariablesPopoverContents));
    }


}

module.exports = cellEditOverlay;
