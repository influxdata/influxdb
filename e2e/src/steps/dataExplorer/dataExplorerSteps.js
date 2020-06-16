const expect = require('chai').expect;

const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const dataExplorerPage = require(__srcdir + '/pages/dataExplorer/dataExplorerPage.js');

class dataExplorerSteps extends influxSteps {

    constructor(driver) {
        super(driver);
        this.dePage = new dataExplorerPage(driver);
    }

    async isLoaded() {
        await this.dePage.isLoaded();
    }

    async verifyIsLoaded() {
        this.assertVisible(await this.dePage.getTimeLocaleDropdown());
        this.assertVisible(await this.dePage.getGraphTypeDropdown());
        this.assertVisible(await this.dePage.getCustomizeGraphButton());
        this.assertVisible(await this.dePage.getSaveAsButton());
        this.assertVisible(await this.dePage.getViewArea());
        this.assertVisible(await this.dePage.getViewRawToggle());
        this.assertVisible(await this.dePage.getAutoRefreshDropdown());
        this.assertVisible(await this.dePage.getTimeRangeDropdown());
        this.assertVisible(await this.dePage.getSubmitQueryButton());
    }

    async chooseBucket(bucket) {
        await this.dePage.getBucketSelector(await this.dePage.getItemFromSelectorList(bucket).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(500); //todo better wait
            });
        }))
    }

    /*
    async chooseMeasurement(measurement, index) {
        await this.dePage.getBuilderCardByIndex(await this.dePage.getItemFromSelectorList(measurement).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(500); //todo better wait
            });
        }), index)
    }
     */

    async chooseItemFromBuilderCard(measurement, index) {
        let card = await this.dePage.getBuilderCardByIndex(parseInt(index));
        await this.dePage.getItemFromSelectorList(measurement).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(500); //todo better wait
            });
        })
    }

    async clickQuerySubmitButton() {
        await this.dePage.getSubmitQueryButton().then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(500); //todo better wait
            });
        })
    }

    async verifyGraphVisible(){
        await this.assertVisible(await this.dePage.getGraphCanvas());
    }

    async hoverOverGraph(){
        await this.dePage.getCanvasLine().then(async canvas => {
            let action = await this.driver.actions();
            let rect = await canvas.getRect();
            let x = parseInt(rect.x);
            let y = parseInt(rect.y);
            let centX = parseInt((rect.width / 2) + x);
            let centY = parseInt((rect.height / 2) + y);
            await action.move({x : centX, y: centY, duration: 1000})
                .perform();
            await this.driver.sleep(200); // todo better wait - let graph update
            //await this.dbdPage.getCellHoverBox().then(async box => {
            //    await this.assertVisible(box);
            //    console.log("DEBUG got cell hover box");
            //})
        });
    }

    async verifyGraphDataPointInfoBox(){
        await this.assertVisible(await this.dePage.getGraphHoverLine());
    }






    async getCurrentGraph(){
        await this.dePage.getCanvasAxes().then(async canvasAxes => {
            if(typeof __dataBuffer.graphAxes === 'undefined') {
                __dataBuffer.graphAxes = [];
            }
            /* eslint-disable require-atomic-updates */
            __dataBuffer.graphAxes = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasAxes);

            //  console.log('DEBUG __dataBuffer.graphCellAxes[' + name + "] " +
            //      __dataBuffer.graphCellAxes[name]);

            await this.dePage.getCanvasLine().then(async canvasLine => {
                if(typeof __dataBuffer.graphLine === 'undefined') {
                    __dataBuffer.graphLine = [];
                }
                __dataBuffer.graphLine = await this.driver
                    .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasLine);

                //    console.log('DEBUG __dataBuffer.graphCellLine[' + name + "] " +
                //        __dataBuffer.graphCellLine[name]);
            });
        });
    }

    async verifyGraphChange(){

        await this.driver.sleep(1000); //troubleshoot canvas update issue

        await this.dePage.getCanvasAxes().then(async canvasAxes => {
            let currentAxes = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasAxes);

            await expect(currentAxes).to.not.equal(__dataBuffer.graphAxes);

            await this.dePage.getCanvasLine().then(async canvasLine => {
                let currentLine = await this.driver
                    .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasLine);
                await expect(currentLine).to.not.equal(__dataBuffer.graphLine);
            });
        });
    }

    async moveToHorizontalFractionOfGraph(fraction){
        let fract = fraction.split('/');
        let denom = fract[1];
        let numer = fract[0];
        await this.dePage.getCanvasLine().then(async canvas => {
            let action = await this.driver.actions();
            let rect = await canvas.getRect();
            let x = parseInt(rect.x);
            let y = parseInt(rect.y);
            let targetX = parseInt(((rect.width/denom) * numer) + x);
            let targetY = parseInt((rect.height / 2) + y);
            await action.move({x: targetX, y: targetY, duration: 1000})
                .perform();

            await this.driver.sleep(200); // todo better wait - let graph update
        });
    }

    async dragToHorizonatalFractionOfGraph(fraction){
        let fract = fraction.split('/');
        let denom = fract[1];
        let numer = fract[0];
        await this.dePage.getCanvasLine().then(async canvas => {
            let action = await this.driver.actions();
            let rect = await canvas.getRect();
            let x = parseInt(rect.x);
            let y = parseInt(rect.y);
            let targetX = parseInt(((rect.width/denom) * numer) + x);
            let targetY = parseInt((rect.height / 2) + y);
            await action.press()
                .move({x: targetX, y: targetY, duration: 1000})
                .release()
                .perform();

            await this.driver.sleep(200); // todo better wait - let graph update
        });
    }

    async clickPointWithinGraphByFractions(fracs, name) {
        let xfract = {};
        let yfract = {};
        xfract.raw = fracs.x.split('/');
        yfract.raw = fracs.y.split('/');
        xfract.denom = xfract.raw[1];
        xfract.numer = xfract.raw[0];
        yfract.denom = yfract.raw[1];
        yfract.numer = yfract.raw[0];
        await this.dePage.getCanvasLine(name).then(async canvas => {
            let action = await this.driver.actions();
            let rect = await canvas.getRect();
            let x = parseInt(rect.x);
            let y = parseInt(rect.y);
            let targetX = parseInt(((rect.width/xfract.denom) * xfract.numer) + x);
            let targetY = parseInt(((rect.height/yfract.denom) * yfract.numer) + y);
            await action.move({x: targetX, y: targetY, duration: 1000})
                .doubleClick()
                .perform();

            await this.driver.sleep(200); // todo better wait - let graph update
        });
    }

    async moveToVerticalFractionOfGraph(fraction){
        let fract = fraction.split('/');
        let denom = fract[1];
        let numer = fract[0];
        await this.dePage.getCanvasLine().then(async canvas => {
            let action = await this.driver.actions();
            let rect = await canvas.getRect();
            let x = parseInt(rect.x);
            let y = parseInt(rect.y);
            let targetX = parseInt((rect.width/2) + x);
            let targetY = parseInt(((rect.height/denom) * numer) + y);
            await action.move({x: targetX, y: targetY, duration: 1000})
                .perform();

            await this.driver.sleep(200); // todo better wait - let graph update
        });
    }

    async dragToVerticalFractionOfGraph(fraction){
        let fract = fraction.split('/');
        let denom = fract[1];
        let numer = fract[0];
        await this.dePage.getCanvasLine().then(async canvas => {
            let action = await this.driver.actions();
            let rect = await canvas.getRect();
            let x = parseInt(rect.x);
            let y = parseInt(rect.y);
            let targetX = parseInt((rect.width/2) + x);
            let targetY = parseInt(((rect.height/denom) * numer) + y);
            await action.press()
                .move({x: targetX, y: targetY, duration: 1000})
                .release()
                .perform();

            await this.driver.sleep(200); // todo better wait - let graph update
        });
    }

    async clickRefreshGraphButton() {
        await this.dePage.getRefreshGraphButton().then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(500); //todo better wait
            });
        })
    }




    async clickRefreshDropdownPaused(){
        await this.dePage.getRefreshDropdownPaused().then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); // todo better wait
            });
        });
    }

    async clickRefreshDropdownActive(){
        await this.dePage.getRefreshDropdownActive().then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); // todo better wait
            });
        });
    }

    async selectRefreshDropdownItem(item){
        await this.dePage.getRefreshDropdownItem(item).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); // todo better wait
            });
        });
    }

    async clickAddQueryButton(){
        await this.dePage.getAddQueryButton().then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); // todo better wait
            });
        });
    }

    async rightClickQueryTabTitle(title) {
        await this.dePage.getQueryTabByName(title).then(async elem => {
            let action = this.driver.actions();

            await action.contextClick(elem).perform();
        });
    }

    async clickQueryTabTitle(title) {
        await this.dePage.getQueryTabByName(title).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); // todo better wait
            });
        });
    }

    async selectQueryTabMenuItem(item){
        await this.dePage.getQueryTabMenuItem(item).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); // todo better wait
            });
        });
    }

    async setQueryTabName(name){
        await this.dePage.getQueryTabNameInput().then(async elem => {
            await this.driver.sleep(200);
            //await elem.clear();
            await elem.sendKeys(name);
        });
    }

    async clickScriptEditorButton(){
        await this.dePage.getScriptEditorButton().then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); // todo better wait
            });
        });
    }

    async pasteIntoScriptEditor(text){
        await this.setMonacoEditorText(await this.dePage.getScriptMonacoEditor(), text);
    }

    async clickViewTypeDropdown(){
        await this.clickAndWait(await this.dePage.getViewTypeDropdown());
    }

    async selectViewType(type){
        await this.dePage.getViewType(type).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); // todo better wait
            });
        });
    }

    async verifySingleStatTextVisible(){
        await this.assertVisible(await this.dePage.getSingleStatText());
    }

    async clickViewRawDataToggle(){
        await this.clickAndWait(await this.dePage.getRawDataToggle());
    }

    async verifyRawDataTableVisible(){
        await this.assertVisible(await this.dePage.getRawDataTable());
    }

    async clickTimeRangeDropdown(){
        await this.clickAndWait(await this.dePage.getTimeRangeDropdown());
    }

    async selectTimeRangeDropdownItem(item){
        await this.clickAndWait(await this.dePage.getTimeRangeDropdownItem(item));
    }

    async searchFunction(funct){
        await this.dePage.getFunctionSearchInput().then(async elem => {
            //await elem.clear();
            await elem.sendKeys(funct);
        });
    }

    async selectFunctionFromList(funct){
        await this.clickAndWait(await this.dePage.getSelectorListFunction(funct));
    }



}

module.exports = dataExplorerSteps;
