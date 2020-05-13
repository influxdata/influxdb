const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const checkStatusHistoryPage = require(__srcdir + '/pages/monitoring/checkStatusHistoryPage.js');
const expect = require('chai').expect;

class checkStatusHistorySteps extends influxSteps {

    constructor(driver) {
        super(driver);
        this.ckHistPage = new checkStatusHistoryPage(__wdriver);
    }

    async isLoaded() {
        await this.ckHistPage.isLoaded();
    }

    async verifyIsLoaded() {
        await this.assertVisible(await this.ckHistPage.getAlertHistoryTitle());
        await this.assertVisible(await this.ckHistPage.getFilterInput());
        await this.assertVisible(await this.ckHistPage.getCanvasGraphAxes());
        await this.assertVisible(await this.ckHistPage.getCanvasGraphContent());
        await this.assertVisible(await this.ckHistPage.getEventTable());
    }

    async verifyMinimumEvents(count){
        await this.ckHistPage.getEventRows().then(async rows => {
            expect(rows.length).to.be.at.least(parseInt(count));
        }).catch(async e => {
            console.warn('Caught error looking for events ' + JSON.stringify(e));
            throw e;
        });
    }

    async verifyEventName(index, name){
        await this.verifyElementContainsText(await this.ckHistPage.getEventRowCheckNameField(index), name)
    }

    async clickEventName(index){
        await this.clickAndWait(await this.ckHistPage.getEventRowCheckNameField(index), async () => {
            await this.driver.sleep(500); //can be slow to load?
        });
    }

    async verifyMinimumCountEventsAtLevel(count, level){
        await this.ckHistPage.getEventRowsAtLevel(level.trim()).then(async rows => {
           expect(rows.length).to.be.at.least(parseInt(count));
        }).catch(async e => {
            console.warn(`Caught error looking for events at level ${level} ${JSON.stringify(e)}`);
            throw e;
        })
    }
}

module.exports = checkStatusHistorySteps;
