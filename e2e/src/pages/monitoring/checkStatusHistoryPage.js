const influxPage = require(__srcdir + '/pages/influxPage.js');
const { By } = require('selenium-webdriver');

const alertHistoryTitle = '[data-testid=alert-history-title]';
const filterInput = '//*[./*[@data-testid=\'input-field--default\']]//*[@data-testid=\'input-field\']';
const eventRows = '.event-row';
const eventRowCheckNameField = '//*[./*[@class=\'event-row\']][%INDEX%]//a';
const eventRowsAtLevel = '//*[./*[@class=\'event-row\']]//*[contains(@class,\'level-table-field--%LEVEL%\')]';

const canvasGraphAxes = 'canvas.giraffe-axes';
const canvasGraphContent = 'canvas.giraffe-layer-line';

const eventTable = '.event-table';

const urlCtx = 'checks';

class checkStatusHistoryPage extends influxPage {

    constructor(driver) {
        super(driver);
    }

    async isLoaded() {
        await super.isLoaded([{type: 'css', selector: alertHistoryTitle },
            {type: 'xpath', selector: filterInput },
            {type: 'css', selector: canvasGraphAxes },
            {type: 'css', selector: canvasGraphContent },
            {type: 'css', selector: eventTable}
        ], urlCtx);
    }


    async getAlertHistoryTitle(){
        return await this.driver.findElement(By.css(alertHistoryTitle));
    }

    async getFilterInput(){
        return await this.driver.findElement(By.xpath(filterInput));
    }

    async getEventRows(){
        return await this.driver.findElements(By.css(eventRows));
    }

    async getEventRowCheckNameField(index){
        return await this.driver.findElement(By.xpath(eventRowCheckNameField.replace('%INDEX%', index)));
    }

    async getCanvasGraphAxes(){
        return await this.driver.findElement(By.css(canvasGraphAxes));
    }

    async getCanvasGraphContent(){
        return await this.driver.findElement(By.css(canvasGraphContent));
    }

    async getEventTable(){
        return await this.driver.findElement(By.css(eventTable));
    }

    async getEventRowsAtLevel(level){
        return await this.driver.findElements(By.xpath(eventRowsAtLevel.replace('%LEVEL%', level)))
    }

}

module.exports = checkStatusHistoryPage;

