const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const checkStatusHistoryPage = require(__srcdir + '/pages/monitoring/checkStatusHistoryPage.js');
const expect = require('chai').expect;

const graphAxesStoreName = 'historyGraphAxes';
const graphContentStoreName = 'historyGraphContent';

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

    async clickEventFilterInput(){
        await this.clickAndWait(await this.ckHistPage.getFilterInput());
    }

    async verifyFilterExamplesDropdownVisible(){
        await this.assertVisible(await this.ckHistPage.getEventFilterExamplesDropdown());
    }

    async verifyFilterExamplesDropdownNotVisible(){
        await this.assertNotPresent(await checkStatusHistoryPage.getEventFilterExamplesDropdownSelector());
    }

    async clickAlertHistoryTitle(){
        await this.clickAndWait(await this.ckHistPage.getAlertHistoryTitle());
    }

    async zoomInOnEventMarkers(){
        await this.ckHistPage.getEventMarkers().then(async markers => {
            let first = await this.ckHistPage.getEventMarkerByIndex(1);
            let last = await this.ckHistPage.getEventMarkerByIndex(markers.length);
            //console.log("DEBUG first " + JSON.stringify(await first.getRect()));
            //console.log("DEBUG last " + JSON.stringify(await last.getRect()));
            await this.driver.executeScript('arguments[0].style.border=\'3px solid red\'', first);
            await this.driver.executeScript('arguments[0].style.border=\'3px solid red\'', last);
            await this.ckHistPage.getCanvasGraphContent().then(async canvas => {
                //console.log("DEBUG canvas " + JSON.stringify(await canvas.getRect()));
                let action = await this.driver.actions();
                let rect = await canvas.getRect();
                let x = parseInt(rect.x);
                let y = parseInt(rect.y);
                //let targetX = parseInt(((rect.width/denom) * numer) + x);
                let targetY = parseInt((rect.height / 2) + y);
                let targetX1 = (await first.getRect()).x - 2;

                //console.log("DEBUG targetX1 " + targetX1 + ' type: ' + typeof(targetX1));
                await action.move({x: parseInt(targetX1), y: targetY, duration: 500})
                    .perform();

                let action2 = await this.driver.actions();
                let targetX2 = (await last.getRect()).x + 2;

                await action2.press()
                    .move({x: parseInt(targetX2), y: targetY, duration: 500})
                    .release()
                    .perform();

                await this.driver.sleep(200); // todo better wait - let graph update
            })
        })
    }

    async getEventsHistoryGraph(){
        await this.ckHistPage.getCanvasGraphAxes().then(async canvasAxes => {
            if(typeof __dataBuffer.graphCellAxes === 'undefined') {
                __dataBuffer.graphCellAxes = [];
            }
            /* eslint-disable require-atomic-updates */
            __dataBuffer.graphCellAxes[graphAxesStoreName] = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasAxes);

            //  console.log('DEBUG __dataBuffer.graphCellAxes[' + graphAxesStoreName + "] " +
            //      __dataBuffer.graphCellAxes[graphAxesStoreName]);

            await this.ckHistPage.getCanvasGraphContent().then(async canvasLine => {
                if(typeof __dataBuffer.graphCellLine === 'undefined') {
                    __dataBuffer.graphCellLine = [];
                }
                __dataBuffer.graphCellLine[graphContentStoreName] = await this.driver
                    .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasLine);

              //      console.log('DEBUG __dataBuffer.graphCellLine[' + graphContentStoreName + "] " +
              //          __dataBuffer.graphCellLine[graphContentStoreName]);
            });
        });
    }

    async verifyEventsHistoryGraphChanged(){
        await this.ckHistPage.getCanvasGraphAxes().then(async canvasAxes => {
            let currentAxes = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasAxes);

            await expect(currentAxes).to.not.equal(__dataBuffer.graphCellAxes[graphAxesStoreName]);

            await this.ckHistPage.getCanvasGraphContent().then(async canvasLine => {
                let currentLine = await this.driver
                    .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasLine);
                await expect(currentLine).to.not.equal(__dataBuffer.graphCellLine[graphContentStoreName]);
            });
        });

    }

    async getEventMarkerTypesAndLocations(){
        await this.ckHistPage.getEventMarkers().then(async markers => {

            __dataBuffer.eventMarkers = [];

            for(let marker of markers){
                //await console.log("DEBUG Marker " + JSON.stringify(await marker.getAttribute('class')) + ' '
                //    + JSON.stringify(await marker.getRect()));
                let rect = await marker.getRect();
                __dataBuffer.eventMarkers.push({ clazz: await marker.getAttribute('class'), rect: rect })
            }
        });
    }

    async verifyEventMarkerLocationChanges(){
        await this.ckHistPage.getEventMarkers().then(async markers => {
           for(let i = 0; i < markers.length; i++){
                //await console.log(`DEBUG __dataBuffer.eventMarkers[${i}]: ${JSON.stringify(__dataBuffer.eventMarkers[i])} `);
                expect(await markers[i].getAttribute('class')).to.equal(__dataBuffer.eventMarkers[i].clazz);
                let rect = await markers[i].getRect();
                expect(rect.x).to.not.equal(__dataBuffer.eventMarkers[i].rect.x)
           }
        });
    }

    async verifyEventToggleOff(event){
        await this.verifyElementContainsClass(await this.ckHistPage.getEventMarkerToggleByType(event), 'eye-closed');
    }

    async verifyEventToggleOn(event){
        await this.verifyElementContainsClass(await this.ckHistPage.getEventMarkerToggleByType(event), 'eye-open');
    }

    async doubleClickHistoryGraph(){
        await this.ckHistPage.getCanvasGraphContent().then(async canvas => {
            let rect = await canvas.getRect();
            let x = parseInt(rect.x);
            let y = parseInt(rect.y);
            let centX = parseInt((rect.width/2) + x);
            let centY = parseInt((rect.height/2) + y);
            let action = this.driver.actions();
            await action.move({x: centX, y: centY, duration: 1000})
                .doubleClick()
                .perform();

            await this.driver.sleep(500); //give graph time to redraw
        })
    }

}

module.exports = checkStatusHistorySteps;
