const influxPage = require(__srcdir + '/pages/influxPage.js');
const { By } = require('selenium-webdriver');

const timeLocaleDropdown = '[data-testid=select-dropdown]';
const graphTypeDropdodwn = '[data-testid=page-control-bar--left] [data-testid=view-type--dropdown]';
const customizeGraphButton = '[data-testid=page-control-bar--left] [data-testid=cog-cell--button]';
const saveAsButton = '//button[./span[text() = \'Save As\']]';
const viewArea = '.time-machine--view';
const viewRawToggle = '[data-testid=raw-data--toggle]';
const autorefreshDropdown = 'div.autorefresh-dropdown';
//const pausedAutorefreshButton = 'button.autorefresh-dropdown--pause'; //Present only when autorefresh is paused - not good candidate for page loade check
const timeRangeDropdown = '//*[@data-testid=\'flex-box\']/div[3]';
//const scriptEditToggle = '[data-testid=switch-to-script-editor] '; //N.B. disappears when in Script edit mode - not good candidate for page load check
//const queryBuildToggle = '[data-testid=switch-to-query-builder]'; //N.B. not present when in Query builder mode - not good candidate for page load check
const submitQueryButton = '[data-testid=time-machine-submit-button]';
const bucketSelector = '[data-testid=bucket-selector]';
const builderCard = '[data-testid=builder-card]';
const graphCanvas = '[data-testid^=giraffe-layer]';

const cellCanvasLine = '//*[contains(@class, \' cell \')][.//*[text()=\'%NAME%\']]//*[@data-testid=\'giraffe-layer-line\']';
const graphHoverLine = '[data-testid=giraffe-layer-hover-line]';
const canvasAxes = '//*[@data-testid=\'giraffe-axes\']'; //'[@data-testid=\'giraffe-axes\']';

const refreshGraphButton = '//*[@class=\'autorefresh-dropdown--pause\'][@data-testid=\'square-button\']'; '//*[@class=\'cell--header\'][./*[text()=\'%NAME%\']]';
const refreshDropdownPaused = '//*[@class=\'autorefresh-dropdown paused\'][.//*[@data-testid=\'dropdown\']]';
const refreshDropdownActive = '//*[@class=\'autorefresh-dropdown\'][.//*[@data-testid=\'dropdown\']]';
const refreshDropdownItem = '[id=\'auto-refresh-%ITEM%\']'; //'//*[@data-testid=\'dropdown-menu--contents\'][.//*[text()=\'%ITEM%\']]'; //*[contains(@class, ' cell ')][.   '//*[id=\'auto-refresh-%ITEM%\'][@data-testid=\'dropdown-item\']';

const addQueryButton = '[class=time-machine-queries--tabs] [data-testid=square-button]';
const queryTabByName = '//*[contains(@class,\'query-tab\')][./*[text()=\'%NAME%\']]';
const queryTabMenuItem = '//*[@data-testid=\'right-click--%ITEM%-tab\']';
const queryTabNameInput = '//*[contains(@class,\'query-tab__active\')][.//*[contains(@class,\'cf-input-xs\')]]';

const scriptEditorButton = '[data-testid=switch-to-script-editor]';

const scriptMonacoEditor = '.inputarea';

const viewTypeDropdown = '[data-testid=page-control-bar--left] [data-testid=\'view-type--dropdown\']';
const viewType = '//*[@class=\'cf-dropdown-item--children\'][.//*[text()=\'%TYPE%\']]';

const singleStatText = '[data-testid=single-stat--text]';
const rawDataToggle = '[data-testid=raw-data--toggle]';
const rawDataTable = '[data-testid=raw-data-table]';

const functionSearchInput = '//*[@data-testid=\'function-selector\'][.//*[contains(@class,\'tag-selector--search\')]]';




//TODO - more controls

const urlCtx = 'data-explorer';

class dataExplorerPage extends influxPage {

    constructor(driver){
        super(driver);
    }


    async isLoaded(){
        await super.isLoaded([{type: 'css', selector: timeLocaleDropdown},
            {type: 'css', selector: graphTypeDropdodwn},
            {type: 'css', selector: customizeGraphButton},
            {type: 'xpath', selector: saveAsButton},
            {type: 'css', selector: viewArea},
            {type: 'css', selector: viewRawToggle},
            {type: 'css', selector: autorefreshDropdown},
            {type: 'xpath', selector: timeRangeDropdown},
            {type: 'css', selector: submitQueryButton}
        ], urlCtx);
    }

    async getTimeLocaleDropdown(){
        return await this.driver.findElement(By.css(timeLocaleDropdown));
    }

    async getGraphTypeDropdown(){
        return await this.driver.findElement(By.css(graphTypeDropdodwn));
    }

    async getCustomizeGraphButton(){
        return await this.driver.findElement(By.css(customizeGraphButton));
    }

    async getSaveAsButton(){
        return await this.driver.findElement(By.xpath(saveAsButton));
    }

    async getViewArea(){
        return await this.driver.findElement(By.css(viewArea));
    }

    async getViewRawToggle(){
        return await this.driver.findElement(By.css(viewRawToggle));
    }

    async getAutoRefreshDropdown(){
        return await this.driver.findElement(By.css(autorefreshDropdown));
    }

    async getTimeRangeDropdown(){
        return await this.driver.findElement(By.xpath(timeRangeDropdown));
    }

    async getSubmitQueryButton(){
        return await this.driver.findElement(By.css(submitQueryButton));
    }

    async getBucketSelector(){
        return await this.driver.findElement(By.css(bucketSelector));
    }

    async getItemFromSelectorList(bucket){
        return await this.driver.findElement(By.css(`[data-testid='selector-list ${bucket}']`));
    }

    async getBuilderCard(){
        return await this.driver.findElements(By.css(builderCard));
    }

    async getBuilderCardByIndex(index){
        return (await this.driver.findElements(By.css(builderCard)))[index - 1];
    }

    async getGraphCanvas(){
        return await this.driver.findElement(By.css(graphCanvas));
    }

    async getCanvasLine(){
        return await this.driver.findElement(By.css(graphCanvas));
    }

    async getGraphHoverLine(){
        return await this.driver.findElement(By.css(graphHoverLine));
    }

    async getCanvasAxes(){
        return await this.driver.findElement(By.xpath(canvasAxes));
    }

    async getRefreshGraphButton(){
        return await this.driver.findElement(By.xpath(refreshGraphButton));
    }

    async getRefreshDropdownPaused(){
        return await this.driver.findElement(By.xpath(refreshDropdownPaused));
    }

    async getRefreshDropdownActive(){
        return await this.driver.findElement(By.xpath(refreshDropdownActive));
    }

    //async getRefreshDropdownItem(item){
     //   return await this.driver.findElement(By.css(`[data-testid='dropdown-item'][id='auto-refresh-${item}']`));
    //}

    async getRefreshDropdownItem(item){
        return await this.driver.findElement(By.css(refreshDropdownItem.replace('%ITEM%', item)));
    }

    async getAddQueryButton(){
        return await this.driver.findElement(By.css(addQueryButton));
    }

    async getQueryTabByName(name){
        return await this.driver.findElement(By.xpath(queryTabByName.replace('%NAME%', name)));
    }

    async getQueryTabMenuItem(item){
        return await this.driver.findElement(By.xpath(queryTabMenuItem.replace('%ITEM%', item)));
    }

    async getQueryTabNameInput(){
        return await this.driver.findElement(By.xpath(queryTabNameInput));
    }

    async getScriptEditorButton(){
        return await this.driver.findElement(By.css(scriptEditorButton));
    }

    async getScriptMonacoEditor(){
        return await this.driver.findElement(By.css(scriptMonacoEditor));
    }

    async getViewTypeDropdown(){
        return await this.driver.findElement(By.css(viewTypeDropdown));
    }

    async getViewType(type){
        return await this.driver.findElement(By.xpath(viewType.replace('%TYPE%', type)));
    }

    async getSingleStatText(){
        return await this.driver.findElement(By.css(singleStatText));
    }

    async getRawDataToggle(){
        return await this.driver.findElement(By.css(rawDataToggle));
    }

    async getRawDataTable(){
        return await this.driver.findElement(By.css(rawDataTable));
    }

    async getTimeRangeDropdownItem(item){
        return await this.driver.findElement(By.css(`[data-testid='dropdown-item-past${item}']`));
    }

    async getFunctionSearchInput(){
        return await this.driver.findElement(By.xpath(functionSearchInput));
    }

    async getSelectorListFunction(funct){
        return await this.driver.findElement(By.css(`[data-testid='selector-list ${funct}']`));
    }




    //TODO - more element getters

}

module.exports = dataExplorerPage;
