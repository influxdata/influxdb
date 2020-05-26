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

    //TODO - more element getters



}

module.exports = dataExplorerPage;
