const expect = require('chai').expect;
const assert = require('chai').assert;
const { Key, until, By } = require('selenium-webdriver');

const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const dashboardPage = require(__srcdir + '/pages/dashboards/dashboardPage.js');
//const basePage = require(__srcdir + '/pages/basePage.js');
const cellEditOverlay = require(__srcdir + '/pages/dashboards/cellEditOverlay.js');
//const influxUtils = require(__srcdir + '/utils/influxUtils.js');

class dashboardSteps extends influxSteps {

    constructor(driver) {
        super(driver);
        this.dbdPage = new dashboardPage(driver);
        this.cedOverlay = new cellEditOverlay(driver);
    }

    async nameDashboard(name){
        await this.dbdPage.getPageTitle().then(async elem => {
            await elem.click().then( async () => {
                await this.dbdPage.getNameInput().then(async input => {
                    await this.clearInputText(input);
                    //await input.clear().then(async () => { // input.clear not working consistently here
                    await input.sendKeys(name + Key.ENTER).then(async () => {
                    });
                    //});
                });
            });
        });
    }

    async verifyDashboardLoaded(name){
        await this.dbdPage.isLoaded();
        await this.dbdPage.getPageTitle().then(async title => {
            await title.getText().then(async text => {
                await expect(text).to.equal(name);
            });
        });

    }

    async verifyDashboardCellVisible(name){
        await this.assertVisible(await this.dbdPage.getCellByName(name));
    }

    async verifyDashboardEmptyDocLinkVisible(){
        await this.assertVisible(await this.dbdPage.getEmptyStateTextLink());
    }

    async verifyDashboardEmptyDocLink(link){
        await this.verifyElementAttributeContainsText(await this.dbdPage.getEmptyStateTextLink(),
            'href', link);
    }

    async verifyDashboardEmptyAddCell(){
        await this.assertVisible(await this.dbdPage.getEmptyStateAddCellButton());
    }

    async clickDashboardTimeLocaleDropdown(){
        await this.clickAndWait(await this.dbdPage.getTimeLocaleDropdown());
    }

    async verifyDashboardDropdownContains(items){
        let itemArr = items.split(',');
        for(let i = 0; i < itemArr.length; i++){
            await this.dbdPage.getDropdownMenuItem(itemArr[i].trim()).then(async elem => {
                assert(true, `${await elem.getText()} is in list`);
            }).catch(() => {
                assert(false, `${itemArr[i]} is in list: `);
            });
        }
    }

    async verifyDashboardDropdownContainsDividers(labels){
        let labelArr = labels.split(',');
        for(let i = 0; i < labelArr.length; i++){
            await this.dbdPage.getdropdownMenuDivider(labelArr[i].trim()).then(async elem => {
                assert(true, `${await elem.getText()} is in list`);
            }).catch(() => {
                assert(false, `${labelArr[i]} is in list`);
            });
        }
    }

    async clickDashboardRefreshDropdown(){
        await this.clickAndWait(await this.dbdPage.getRefreshRateDropdown());
    }

    async clickDashboardTimeRangeDropdown(){
        await this.clickAndWait(await this.dbdPage.getTimeRangeDropdown());
    }

    async verifyDashboardTimeRangeDropdownSelected(value){
        await this.verifyElementText(await this.dbdPage.getTimeRangeDropdownSelected(), value)
    }

    async selectDashboardTimeRange(item){
        await this.dbdPage.getTimeRangeDropdownItem(item).then(async elem => {
            await this.scrollElementIntoView(elem).then(async () => {
                await this.clickAndWait(elem, async () => {
                    await this.driver.sleep(1500); //give cells chance to reload - TODO better wait
                });
            });
        });
    }

    async clickCreateCellEmpty(){
        //todo wait for buckets to be loaded ---
        await this.clickAndWait(await this.dbdPage.getEmptyStateAddCellButton(), async() => {
            await this.driver.sleep(1000); /*this.driver.wait(
                until.elementLocated(By.css(cellEditOverlay.getBucketSelectSearchSelector().selector))
            );*/  //for some reason wait with until still sometimes overruns subsequent steps
        });
    }

    async clickHeaderAddCellButton(){
        await this.clickAndWait(await this.dbdPage.getAddCellButtonHeader(), async() => {
            await this.driver.sleep(1000); //see comment above in clickCreateCellEmpty()
        })
    }

    async verifyCellNotPresent(name){
        await this.assertNotPresent(await dashboardPage.getCellSelectorByName(name));
    }

    async verifyEmptyGraphMessage(name){
        await this.assertVisible(await this.dbdPage.getCellEmptyGraphMessage(name));
    }

    async verifyEmptyGraphNoResults(name){
        await this.assertVisible(await this.dbdPage.getCellEmptyGraphNoResults(name));
    }

    async verifyCellContainsGraphError(name){
        await this.assertVisible(await this.dbdPage.getCellEmptyGraphError(name));
    }

    async verifyCellContainsGraph(name){
        await this.assertVisible(await this.dbdPage.getCellCanvasAxes(name));
        await this.assertVisible(await this.dbdPage.getCellCanvasLine(name));
    }

    async getCellMetrics(name){
        await this.dbdPage.getCellByName(name).then(async cell => {
            await cell.getRect().then(async rect => {
                // console.log("DEBUG rect for " + name + " " + JSON.stringify(rect))
                /* eslint-disable no-undef */
                if(typeof __dataBuffer.rect === 'undefined'){
                    __dataBuffer.rect = [];
                }
                __dataBuffer.rect[name] = rect;
                //debug why resize not saved
                //await influxUtils.signInAxios('admin');
                //let dashboards = await influxUtils.getDashboards();
                //console.log("DEBUG dashboards " + JSON.stringify(dashboards));

            });
        });
    }

    async getCurrentGraphOfCell(name){
        await this.dbdPage.getCellCanvasAxes(name).then(async canvasAxes => {
            if(typeof __dataBuffer.graphCellAxes === 'undefined') {
                __dataBuffer.graphCellAxes = [];
            }
            /* eslint-disable require-atomic-updates */
            __dataBuffer.graphCellAxes[name] = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasAxes);

            //  console.log('DEBUG __dataBuffer.graphCellAxes[' + name + "] " +
            //      __dataBuffer.graphCellAxes[name]);

            await this.dbdPage.getCellCanvasLine(name).then(async canvasLine => {
                if(typeof __dataBuffer.graphCellLine === 'undefined') {
                    __dataBuffer.graphCellLine = [];
                }
                __dataBuffer.graphCellLine[name] = await this.driver
                    .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasLine);

            //    console.log('DEBUG __dataBuffer.graphCellLine[' + name + "] " +
            //        __dataBuffer.graphCellLine[name]);
            });
        });
    }

    async verifyCellGraphChange(name){

        await this.driver.sleep(1000); //troubleshoot canvas update issue

        await this.dbdPage.getCellCanvasAxes(name).then(async canvasAxes => {
            let currentAxes = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasAxes);

            await expect(currentAxes).to.not.equal(__dataBuffer.graphCellAxes[name]);

            await this.dbdPage.getCellCanvasLine(name).then(async canvasLine => {
                let currentLine = await this.driver
                    .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasLine);
                await expect(currentLine).to.not.equal(__dataBuffer.graphCellLine[name]);
            });
        });
    }

    async verifyCellGraphNoChange(name){

        await this.driver.sleep(1000); //troubleshoot canvas update issue

        await this.dbdPage.getCellCanvasAxes(name).then(async canvasAxes => {
            let currentAxes = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasAxes);

            await expect(currentAxes).to.equal(__dataBuffer.graphCellAxes[name]);

            await this.dbdPage.getCellCanvasLine(name).then(async canvasLine => {
                let currentLine = await this.driver
                    .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasLine);
                await expect(currentLine).to.equal(__dataBuffer.graphCellLine[name]);
            });
        });

    }

    async verifyCellGraphVisible(name){
        await this.assertVisible(await this.dbdPage.getCellCanvasLine(name));
        await this.assertVisible(await this.dbdPage.getCellCanvasAxes(name));
    }

    async compareCellGraphs(name1, name2, equal = true){
        await this.dbdPage.getCellCanvasLine(name1).then(async canvas1 => {
            let line1 = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', canvas1);
            await this.dbdPage.getCellCanvasLine(name2).then(async canvas2 => {
                let line2 = await this.driver
                    .executeScript('return arguments[0].toDataURL(\'image/png\');', canvas2);

                if(equal) {
                    await expect(line1).to.equal(line2);
                }else{
                    await expect(line1).to.not.equal(line2);
                }
            });
        });
    }

    async toggleDashboardCellContextMenu(name){
        await this.clickAndWait(await this.dbdPage.getCellContextToggleByName(name), async () => {
            await this.driver.wait(
                until.elementLocated(By.css(dashboardPage.getCellPopoverContentsSelector().selector))
            );
        });
    }

    async toggle2ndDashboardCellContextMenu(name){
        await this.dbdPage.getCellsByName(name).then( async cells => {
            await this.clickAndWait(await cells[1]
                .findElement(By.xpath('.//*[@data-testid=\'cell-context--toggle\']')), async () => {
                await this.driver.sleep(1000); // todo - better wait sometimes slow to load
            });
        });
    }

    async clickDashboardPopOverlayAddNote(){
        await this.clickAndWait(await this.dbdPage.getCellPopoverContentsAddNote(), async () => {
            //await this.driver.wait(
            //until.elementLocated(By.css(basePage.getPopupBodySelector().selector)) // not working consistentlly
            //);
            await this.driver.sleep(1000); //sometimes slow and then overrun by downstream steps
        });
    }

    async clickNotePopupCance(){
        await this.clickAndWait(await this.dbdPage.getNotePopupCancel());
    }

    async clickDashboardPopOverlayEditNote(){
        await this.clickAndWait(await this.dbdPage.getcellPopoverContentsEditNote(), async () => {
            await this.driver.sleep(1000);
        })
    }

    async clickDashboardPopOverlayConfigure(){
        await this.clickAndWait(await this.dbdPage.getCellPopoverContentsConfigure(), async () => {
            /*await this.driver.wait(
                until.elementLocated(By.css(cellEditOverlay.getTimeMachineOverlay().selector))
            );*/ // this wait not working reliably.  so for now use sleep
            await this.driver.sleep(1500); // todo better wait
        });
    }

    async clickDashboardPopOverlayDelete(){
        await this.clickAndWait(await this.dbdPage.getCellPopoverContentsDelete());
    }

    async clickDashboardPopOverlayDeleteConfirm(){
        await this.clickAndWait(await this.dbdPage.getCellPopoverContentsDeleteConfirm());
    }

    async clickDashboardPopOverlayClone(){
        await this.clickAndWait(await this.dbdPage.getCellPopoverContentsClone());
    }

    async verifyEditNotePopupLoaded(){
        await this.assertVisible(await this.dbdPage.getPopupBody());
        await this.verifyElementContainsText(await this.dbdPage.getPopupTitle(), 'Edit Note');
        await this.assertVisible(await this.dbdPage.getNotePopupCodeMirror());
        await this.assertVisible(await this.dbdPage.getPopupDismiss());
        await this.assertVisible(await this.dbdPage.getNotePopupCancel());
        await this.assertVisible(await this.dbdPage.getNotePopupSave());
        await this.assertVisible(await this.dbdPage.getNotePopupNoDataToggle());
        await this.assertVisible(await this.dbdPage.getNotePopupEditorPreview());
    }

    async verifyMainEditNotePopupLoaded(state){
        await this.assertVisible(await this.dbdPage.getPopupBody());
        await this.verifyElementContainsText(await this.dbdPage.getPopupTitle(), `${state} Note`);
        await this.assertVisible(await this.dbdPage.getNotePopupCodeMirror());
        await this.assertVisible(await this.dbdPage.getPopupDismiss());
        await this.assertVisible(await this.dbdPage.getNotePopupCancel());
        await this.assertVisible(await this.dbdPage.getNotePopupSave());
        await this.assertVisible(await this.dbdPage.getNotePopupEditorPreview());
        await this.assertVisible(await this.dbdPage.getNotePopupGuideLink());
    }

    async setCellNotePopupCodeMirrorText(text){
        await this.setCodeMirrorText(await this.dbdPage.getNotePopupCodeMirror(), text);
    }

    async verifyCellNotPopupPreviewContains(text){
        await this.dbdPage.getNotePopupEditorPreviewText().then(async elem => {
            await expect(await elem.getText()).to.include(text);
        });
    }

    async clickCellNotePopupSave(){
        await this.clickAndWait(await this.dbdPage.getPopupSaveSimple(), async () => {
            await this.driver.wait(
                until.stalenessOf(await this.dbdPage.getPopupOverlay())
            );
        });
    }

    async verifyCellHasNoteIndicator(name){
        await this.assertVisible(await this.dbdPage.getCellNoteByName(name));
    }

    async clickCellNoteIndicator(name){
        await this.clickAndWait(await this.dbdPage.getCellNoteByName(name));
    }

    async verifyContentsOfCellNote(text){
        await this.dbdPage.getNotePopoverContents().then(async contents => {
            await expect(await contents.getText()).to.include(text);
        });
    }

    async verifyNotePopupMarkdownPreviewContains(tag, content){
        await this.verifyElementText(await this.dbdPage.getNotePopupEditorPreviewTag(tag), content);
    };

    async verifyNoteCellContains(tag,content){
        await this.verifyElementText(await this.dbdPage.getNoteCellMarkdownTag(tag), content);
    }

    async clickCellTitle(name){
        await this.clickAndWait(await this.dbdPage.getCellTitle(name));
    }

    async verifyCellNotePopoverNotPresent(){
        await this.assertNotPresent(dashboardPage.getNotePopoverSelector());
    }

    async verifyCellContentPopoverItemEditNote(){
        await this.verifyElementContainsText(await this.dbdPage.getCellPopoverContentsAddNote(), 'Edit Note');
    }

    async verifyCellContentPopoverNotPresent(){
        await this.assertNotPresent(dashboardPage.getCellPopoverContentsSelector());
    }

    async verifyCodeMirrorContainsText(text){
        expect(await this.getCodeMirrorText(await this.dbdPage.getNotePopupCodeMirror()))
            .to.include(text);
    }

    async clearCellNotePopupCodeMirror(){
        await this.setCodeMirrorText(await this.dbdPage.getNotePopupCodeMirror(), '');
    }

    async verifyCellNotePopupMarkupPreviewNoText(){
        await this.dbdPage.getNotePopupEditorPreviewText().then(async text => {
            let strText = await text.getText();
            expect(strText.length).to.equal(0);
        });
    }

    async moveDashboardCell(name, deltaCoords){
        //await this.clickAndWait(await this.dbdPage.getCellHandleByName(name));
        await this.dbdPage.getCellHandleByName(name).then( async cell => {
            let action = await this.driver.actions();
            let rect = await cell.getRect();
            let x = parseInt(rect.x);
            let y = parseInt(rect.y);
            let dx = parseInt(deltaCoords.dx);
            let dy = parseInt(deltaCoords.dy);
            await action
                .move({x: x, y: y, duration: 1000})
                .press()
                .move({x:x + dx , y: y + dy, duration: 1000})
                .release()
                .perform();

            await this.driver.sleep(1000);

        });
    }

    async verifyCellPositionChange(name, deltaCoords){
        // Use tolerance because of snap to grid feature
        let tolerance = 50; //can be +/- 50 px
        await this.dbdPage.getCellByName(name).then(async cell => {
            let rect = await cell.getRect();
            //console.log("DEBUG rect for " + name + " " + JSON.stringify(rect))
            let x = parseInt(rect.x);
            let y = parseInt(rect.y);
            let dx = parseInt(deltaCoords.dx);
            let dy = parseInt(deltaCoords.dy);
            let expx = parseInt(__dataBuffer.rect[name].x) + dx;
            let expy = parseInt(__dataBuffer.rect[name].y) + dy;
            expect(Math.abs(expx - x )).to.be.below(tolerance);
            expect(Math.abs(expy - y )).to.be.below(tolerance);
        });
    }

    async resizeDashboardCell(name, deltaSize){

        await this.dbdPage.getCellResizerByName(name).then(async resizer => {
            let action = this.driver.actions();
            let rect = await resizer.getRect();
            //console.log("DEBUG rect " + JSON.stringify(rect));
            let x = parseInt(rect.x);
            let y = parseInt(rect.y);
            //console.log("DEBUG x:" + x + " y:" + y);
            let dw = parseInt(deltaSize.dw);
            let dh = parseInt(deltaSize.dh);
            await action
                .move({x: x, y: y, duration: 1000})
                .press()
                .move({x:x + dw , y: y + dh, duration: 1000})
                .release()
                .perform();
            await this.driver.sleep(200); //slight wait for animation
            await resizer.click();

            //debug why resize not saved
            //await influxUtils.signInAxios('admin');
            //let dashboards = await influxUtils.getDashboards();
            //console.log("DEBUG dashboards " + JSON.stringify(dashboards));


        });
    }

    async verifyDashboardCellSizeChange(name, deltaSize){
        // Use tolerance because of snap to grid feature
        let tolerance = 50; //can be +/- 50 px
        await this.dbdPage.getCellByName(name).then(async cell => {

            //debug why resize not saved
            //await influxUtils.signInAxios('admin');
            //let dashboards = await influxUtils.getDashboards();
            //console.log("DEBUG dashboards " + JSON.stringify(dashboards));

            let rect = await cell.getRect();
            //console.log("DEBUG rect " + JSON.stringify(rect))
            let width = parseInt(rect.width);
            let height = parseInt(rect.height);
            let dw = parseInt(deltaSize.dw);
            let dh = parseInt(deltaSize.dh);
            let exph = parseInt(__dataBuffer.rect[name].height) + dh;
            let expw = parseInt(__dataBuffer.rect[name].width) + dw;
            expect(Math.abs(exph - height )).to.be.below(tolerance);
            expect(Math.abs(expw - width )).to.be.below(tolerance);
        });
    }

    async hoverGraphOfCell(name){
        await this.dbdPage.getCellCanvasLine(name).then(async canvas => {
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

    async verifyCellGraphDataPointInfoBox(){
        await this.assertVisible(await this.dbdPage.getCellHoverBox());
    }

    async moveToHorizontalFractionOfGraphCell(fraction, name){
        let fract = fraction.split('/');
        let denom = fract[1];
        let numer = fract[0];
        await this.dbdPage.getCellCanvasLine(name).then(async canvas => {
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

    async dragToHorizonatlFractionOfGraphCell(fraction, name){
        let fract = fraction.split('/');
        let denom = fract[1];
        let numer = fract[0];
        await this.dbdPage.getCellCanvasLine(name).then(async canvas => {
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

    async moveToVerticalFractionOfGraphCell(fraction, name){
        let fract = fraction.split('/');
        let denom = fract[1];
        let numer = fract[0];
        await this.dbdPage.getCellCanvasLine(name).then(async canvas => {
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

    async dragToVerticalFractionOfGraphCell(fraction, name){
        let fract = fraction.split('/');
        let denom = fract[1];
        let numer = fract[0];
        await this.dbdPage.getCellCanvasLine(name).then(async canvas => {
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

    async clickPointWithinCellByFractions(fracs, name) {
        let xfract = {};
        let yfract = {};
        xfract.raw = fracs.x.split('/');
        yfract.raw = fracs.y.split('/');
        xfract.denom = xfract.raw[1];
        xfract.numer = xfract.raw[0];
        yfract.denom = yfract.raw[1];
        yfract.numer = yfract.raw[0];
        await this.dbdPage.getCellCanvasLine(name).then(async canvas => {
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

    async verifyCountCellsNamed(name, ct){
        await this.dbdPage.getCellsByName(name).then(async cells => {
            expect(cells.length).to.equal(ct);

        });
    }

    async hoverOverCellErrorIcon(name){
        await this.hoverOver(await this.dbdPage.getCellEmptyGraphErrorIcon(name));
    }

    async verifyEmptyCellErrorPopoverMessage(msg){
        await this.verifyElementContainsText(await this.dbdPage.getEmptyGraphPopoverContents(), msg);
    }

    async verifyCellErrorMessage(name, msg){
        await this.verifyElementContainsText(await this.dbdPage.getCellEmptyGraphError(name), msg);
    }

    async verifyVariablesButtonActive(){
        await this.verifyElementContainsClass(await this.dbdPage.getVariablesButton(), 'cf-button-secondary');
    }

    async verifyVariablesButtonInactive(){
        await this.verifyElementContainsClass(await this.dbdPage.getVariablesButton(), 'button-default')
    }

    async clickValueDropdownOfVar(varname){
        await this.clickAndWait(await this.dbdPage.getVariableValueDropdownButtonForVar(varname));
    }

    async verifyVariableDropdownContents(varname,items){
        let list = items.split(',');
        for(let i = 0; i < list.length; i++){
            await this.assertVisible(await this.dbdPage.getVariableValueDropdownItem(varname,list[i].trim()));
        }
    }

    async verifySlectedValueOfVariable(varname, item){
        await this.verifyElementText(await this.dbdPage.getVariableValueDropdownButtonForVar(varname), item);
    }

    async clickItemOfVariableValueDropdown(item, varname){
        await this.clickAndWait(await this.dbdPage.getVariableValueDropdownItem(varname, item));
    }

    async verifyVariableValuesDropdownVisible(varname){
        await this.assertVisible(await this.dbdPage.getVariableValueDropdownButtonForVar(varname));
    }

    async verifyVariableValuesDropdownNotVisible(varname){
        await this.assertNotPresent(await dashboardPage.getVariableValueDropdownButtonForVarSelector(varname));
    }

    async clickVariablesButton(){
        await this.clickAndWait(await this.dbdPage.getVariablesButton());
    }

    async clickDashboardMainAddNote(){
        await this.clickAndWait(await this.dbdPage.getAddNoteButton(), async() => {
            await this.driver.sleep(1000); //troubleshoot occasional problems in CircleCI
        });
    }

}

module.exports = dashboardSteps;
