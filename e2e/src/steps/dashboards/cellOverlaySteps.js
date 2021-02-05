const fs = require('fs');
const expect = require('chai').expect;
const Key = require('selenium-webdriver').Key;
const { By, Origin } = require('selenium-webdriver');

const influxUtils = require(__srcdir + '/utils/influxUtils.js');

const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const cellEditOverlay = require(__srcdir + '/pages/dashboards/cellEditOverlay.js');

class cellOverlaySteps extends influxSteps {

    constructor(driver) {
        super(driver);
        this.cellOverlay = new cellEditOverlay(driver);
    }

    async verifyCellOverlayIsLoaded(title){
        await this.cellOverlay.isLoaded();
        await this.verifyElementContainsText(await this.cellOverlay.getCellTitle(), title);
    }

    async nameDashboardCell(name){
        await this.cellOverlay.getCellTitle().then(async elem => {
            await elem.click().then(async () => {
                await this.cellOverlay.getCellNameInput().then(async input =>{
                    await this.clearInputText(input).then(async () => {
                        await this.typeTextAndWait(input, name + Key.ENTER);
                    });
                });
            });
        });
    }

    async clickDashboardCellEditCancel(){
        await this.clickAndWait(await this.cellOverlay.getEditCancel());
    }

    async clickDashboardCellSave(){
        await this.clickAndWait(await this.cellOverlay.getSaveCell());
    }

    async clickCellEditTimeRangeDropdown(){
        await this.clickAndWait(await this.cellOverlay.getTMTimeRangeDropdown());
    }

    async selectCellEditTimeRangeItem(item){
        let itemToken = await item.replace(/\s/g,'').toLowerCase();
        await this.cellOverlay.getTMTimeRangeDropdownItem(itemToken).then(async elem => {
            await this.scrollElementIntoView(elem).then(async () => {
                await this.clickAndWait(await this.cellOverlay
                    .getTMTimeRangeDropdownItem(itemToken));
            });
        });
    }

    async clickCellEditScriptEditorButton(){
        await this.clickAndWait(await this.cellOverlay.getSwitchToScriptEditor());
    }

    async pasteIntoCellEditScriptEditor(text){
        //await this.setCodeMirrorText(await this.cellOverlay.getScriptEditorCodeMirror(), escapedTxt);
        //await this.setMonacoEditorText(await this.cellOverlay.getScriptMonacoEditor(), escapedTxt);
        await this.setMonacoEditorText(await this.cellOverlay.getScriptMonacoEditor(), text);
    }

    async clearCellEditScriptEditor(){
        //        await this.setCodeMirrorText(await this.cellOverlay.getScriptEditorCodeMirror(), "");
        await this.driver.sleep(1000);
        //await this.setMonacoEditorText(await this.cellOverlay.getScriptMonacoEditor(), "");
        await this.clearMonacoEditorText(await this.cellOverlay.getScriptMonacoEditor());
    }

    async clickCellEditSubmitButton(){
        await this.clickAndWait(await this.cellOverlay.getTimemachineSubmit(), async () => {
            await this.driver.sleep(1500); //todo better wait - sec and half to load for now
        });
    }

    async verifyCellEditSubmitDisabled(){
        await this.verifyElementDisabled(await this.cellOverlay.getTimemachineSubmit());
    }

    async verifyCellEditSubmitEnabled(){
        await this.verifyElementEnabled(await this.cellOverlay.getTimemachineSubmit());
    }

    async getCurrentCellEditPreviewGraph(){

        if(await this.isPresent(cellEditOverlay.getGraphCanvasSelector())) {
            await this.cellOverlay.getGraphCanvas().then(async canvas => {
                /* eslint-disable no-undef */
                __dataBuffer.graphEditCanvas = await this.driver
                    .executeScript('return arguments[0].toDataURL(\'image/png\');', canvas);
                console.log('DEBUG __dataBuffer.graphEditCanvas ' + __dataBuffer.graphEditCanvas);
                await this.cellOverlay.getGraphCanvasAxes().then(async axes => {
                    __dataBuffer.graphEditCanvasAxes = await this.driver
                        .executeScript('return arguments[0].toDataURL(\'image/png\');', axes);
                });
            });
        }else{
            __dataBuffer.graphEditCanvas = '';
        }
    }

    async verifyCellEditPreviewGraphVisible(){
        await this.assertVisible(await this.cellOverlay.getGraphCanvas());
    }

    async verifyCellEditPreviewAxesVisible(){
        await this.assertVisible(await this.cellOverlay.getGraphCanvasAxes());
    }

    async verifyCellEditPreviewGraphChanged(){
        if(await this.isPresent(cellEditOverlay.getGraphCanvasSelector())) {
            await this.cellOverlay.getGraphCanvas().then(async canvas => {
                let currentCanvas = await this.driver
                    .executeScript('return arguments[0].toDataURL(\'image/png\');',
                        canvas);
                //visual DEBUG Below
                //await this.writeBase64ToPNG('debug.png', currentCanvas);
                //this.writeBase64ToPNG('canvas.png', __dataBuffer.graphEditCanvas);
                expect(currentCanvas).to.not.equal(__dataBuffer.graphEditCanvas);
                //Axes may or maynot change TODO look at axes comparison
                /*await this.cellOverlay.getGraphCanvasAxes().then(async axes => {
                    let currentAxes = await this.driver
                        .executeScript('return arguments[0].toDataURL(\'image/png\').substring(21);', axes);
                    //visual DEBUG below
                    await this.writeBase64ToPNG('axes.png', currentAxes);
                })*/
            });
        }else{
            throw new Error('Cell Edit Preview Graph Not Found');
        }
    }

    async clickCellEditSaveButton(){
        await this.clickAndWait(await this.cellOverlay.getSaveCell());
    }

    async clickCellEditName(){
        await this.clickAndWait(await this.cellOverlay.getCellTitle());
    }

    async updateCellName(name){
        await this.clearInputText(await this.cellOverlay.getCellNameInput());
        await this.typeTextAndWait(await this.cellOverlay.getCellNameInput(), name);
    }

    async clickViewTypeDropdown(){
        await this.clickAndWait(await this.cellOverlay.getViewTypeDropdown());
    }

    async verifyViewTypeListContents(itemList){
        let list = itemList.split(',');
        for(let i = 0; i < list.length; i++){
            let elem = await this.cellOverlay.getViewTypeItem(list[i]);
            await this.scrollElementIntoView(elem);
            await this.assertVisible(elem);
        }
    }

    async verifyViewTypeListNotPresent(){
        await this.assertNotPresent(cellEditOverlay.getViewTypeListContentsSelector());
    }

    async clickCellViewCustomize(){
        await this.clickAndWait(await this.cellOverlay.getCustomizeButton());
    }

    async verifyViewOptionsContainerVisible(){
        await this.assertVisible(await this.cellOverlay.getViewOptionsContainer());
    }

    async verifyCellCustomizeButtonHighlight(){
        await this.verifyElementContainsClass(await this.cellOverlay.getCustomizeButton()
            , 'button-primary');
    }

    async verifyViewOptionsContainerNotPresent(){
        await this.assertNotPresent(cellEditOverlay.getViewOptionsContainerSelector());
    }

    async verifyCustomizeButtonNoHighlightd(){
        await this.verifyElementDoesNotContainClass(await this.cellOverlay.getCustomizeButton()
            , 'button-primary');
    }

    async verifyTMViewEmptyQueriesGraphVisible(){
        await this.assertVisible(await this.cellOverlay.getTMViewEmptyGraphQueries());
    }

    async verifyTMViewNoResultsVisible(){
        await this.assertVisible(await this.cellOverlay.getTMViewNoResults());
    }

    async clickTMAutorefreshDropdown(){
        await this.clickAndWait(await this.cellOverlay.getTMAutorefreshDropdown());
    }

    async verifyAutorefreshListContents(itemList){
        let list = itemList.split(',');
        for(let i = 0; i < list.length; i++){
            await this.assertVisible(await this.cellOverlay.getTMAutorefreshItem(list[i]));
        }
    }

    async clickTMAutorefreshItem(item){
        await this.clickAndWait(await this.cellOverlay.getTMAutorefreshItem(item));
    }

    async verifyTMAutorefreshForceButtonNotPresent(){
        await this.assertNotPresent(cellEditOverlay.getTMAutorefreshForceButtonSelector());
    }

    async verifyTMAutorefreshForceButtonVisible(){
        await this.assertVisible(await this.cellOverlay.getTMAutorefreshForceButton());
    }

    async verifyTMAutorefreshDropdownSelected(selected){

        if(selected.toLowerCase() === 'paused'){
            await this.assertVisible(await this.cellOverlay.getTMAutorefreshDropdownPaused());
        }else {

            await this.cellOverlay.getTMAutorefreshDropdownSelected().then(async elem => {
//           console.log("DEBUG selected #" + await elem.getText() + "#");
                await this.driver.executeScript('arguments[0].style.border=\'3px solid red\'', elem);
                console.log("DEBUG selected #" + JSON.stringify(elem) + "#");
            });
            await this.verifyElementText(await this.cellOverlay.getTMAutorefreshDropdownSelected(), selected)
        }
    }

    async clickTMForceRefresh(){
        await this.clickAndWait(await this.cellOverlay.getTMAutorefreshForceButton());
    }

    async verifyTMTimeRangeDropdownList(itemList){
        let list = itemList.split(',');
        for(let i = 0; i < list.length; i++){
            let elem = await this.cellOverlay
                .getTMTimeRangeDropdownItem(list[i]
                    .replace(/\s/g,'')
                    .toLowerCase());
            await this.scrollElementIntoView(elem);
            await this.assertVisible(elem);
        }
    }

    async verifyTMTimeRangeDropdownListNotPresent(){
        await this.assertNotPresent(cellEditOverlay.getTMTimeRangeDropdownContentsSelector());
    }

    async verifyTMQueryBuilderVisible(){
        await this.assertVisible(await this.cellOverlay.getTMQueryBuilder());
    }

    async verifyTMQueryBuilderSwitchWarnNotPresent(){
        await this.assertNotPresent(cellEditOverlay.getTMSwitchToQBuilderWarnSelector());
    }

    async verifyTMQueryBuilderSwitchWarnVisible(){
        await this.assertVisible(await this.cellOverlay.getTMSwitchToQBuilderWarn());
    }

    async verifyTMFluxEditorVisible(){
        await this.assertVisible(await this.cellOverlay.getTMFluxEditor());
    }

    async clickTMSwitch2QBuilder(){
        await this.clickAndWait(await this.cellOverlay.getTMSwitchToQueryBuilder());
    }

    async clickTMSwitch2QBuilderConfirm(){
        await this.clickAndWait(await this.cellOverlay.getTMSwitchToQBuilderConfirm());
    }

    async clickTMFluxEditor(){
        await this.clickAndWait(await this.cellOverlay.getTMFluxEditor());
    }

    async clickTMFilterFunctionsInput(){
        await this.clickAndWait(await this.cellOverlay.getTMQEFunctionFilter());
    }

    async verifyTMFluxEditorNotPresent(){
        await this.assertNotPresent(cellEditOverlay.getTMFluxEditorSelector());
    }

    async verifyTMBucketListContents(bucketList){
        let list = bucketList.split(',');
        for(let i = 0; i < list.length; i++){
            await this.assertVisible(await this.cellOverlay.getTMBucketSelectorBucket(list[i].trim()));
        }
    }

    async clickTMBucketSelectorItem(item){
        await this.clickAndWait(await this.cellOverlay.getTMBucketSelectorBucket(item.trim()),
            async () => { await this.driver.sleep(1000); }); //slow to load sometimes?
    }

    async verifyBucketNotInTMBucketList(bucket){
        await this.assertNotPresent(cellEditOverlay.getTMBucketSelectorBucketSelector(bucket));
    }

    async filterBucketListContents(value){
        await this.clearInputText(await this.cellOverlay.getTMBucketSelectorFilter());
        await this.typeTextAndWait(await this.cellOverlay.getTMBucketSelectorFilter(), value);
    }

    async clearBucketSelectorFilter(){
        await this.clearInputText(await this.cellOverlay.getTMBucketSelectorFilter());
    }

    async verifyTMBuilderCardsSize(count){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await expect(cards.length).to.equal(parseInt(count));
    }

    async verifyItemsInBuilderCard(index,items){
        let cards = await this.cellOverlay.getTMBuilderCards();
        let list = items.split(',');
        for(let i = 0; i < list.length; i++){
            await this.assertVisible(await cards[parseInt(index) - 1]
                .findElement(By.css(`[data-testid='selector-list ${list[i].trim()}']`)));
        }
    }

    async verifyItemSelectedInBuilderCard(index, item){
        let card = await this.cellOverlay.getTMBuilderCardByIndex(parseInt(index));
        await this.verifyElementContainsClass(await card.findElement(By.css(`[data-testid='selector-list ${item.trim()}']`)),
            'selected');

    }

    async verifyEmptyTagsInBuilderCard(index){
        await this.cellOverlay.getTMBuilderCardByIndex(index).then(async elem => {
            await this.assertVisible(await elem.findElement(By.css('[data-testid=\'empty-tag-keys\']')));
        });
    }

    async verifyNoSelectedTagsInBuilderCard(index){
        await this.cellOverlay.getTMBuilderCardByIndex(index).then(async elem => {
            await elem.findElements(By.css('[data-testid^=selector-list][class*=selected]'))
                .then(async items => {
                    await expect(await items.length).to.equal(0);
                });
        });
    }

    async verifySelectorCountInBuilderCard(index,value){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await cards[parseInt(index) - 1]
            .findElement(By.css('.tag-selector--count'))
            .then(async elem => {
                await this.verifyElementContainsText(elem, value);
            });
    }

    async verifyItemNotInBuilderCard(index,item){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await cards[parseInt(index) - 1]
            .findElements(By.css(`[data-testid='selector-list ${item.trim()}']`))
            .then(async elems => {
                expect(await elems.length).to.equal(0);
            });
    }

    async clickTagSelectorOfBuilderCard(index){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await this.clickAndWait(await cards[parseInt(index) - 1]
            .findElement(By.css('[data-testid=tag-selector--dropdown-button]')));
    }

    async verifyItemsInBuilderCardTagSelector(index,items){
        let cards = await this.cellOverlay.getTMBuilderCards();
        let list = items.split(',');
        for(let i = 0; i < list.length; i++){
            await this.assertVisible(await cards[parseInt(index) - 1]
                .findElement(By.css(`[data-testid='searchable-dropdown--item ${list[i].trim()}']`)));
        }
    }

    async clickTagSelectorDropdownItemInBuilderCard(item,index){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await this.clickAndWait(await cards[parseInt(index) - 1]
            .findElement(By.css(`[data-testid='searchable-dropdown--item ${item.trim()}']`)));
    }

    async clickTagInBuilderCard(tag, cardIndex){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await this.clickAndWait(await cards[parseInt(cardIndex) - 1]
            .findElement(By.css(`[data-testid='selector-list ${tag}']`)));
    }

    async clickBuilderCardDelete(index){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await this.clickAndWait(await cards[parseInt(index) - 1]
            .findElement(By.css('.builder-card--delete')));
    }

    async filterBuilderCardListContents(index,term){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await this.typeTextAndWait(await cards[parseInt(index) - 1]
            .findElement(By.css('[data-testid=\'input-field\']')), term,
        async () => { await this.driver.sleep(2000); }); //can be slow to update
        //this.driver.sleep(2000); //DEBUG
    }

    async clearTagsFilterInBuilderCard(index){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await this.clearInputText(await cards[parseInt(index) - 1]
            .findElement(By.css('[data-testid=\'input-field\']')));
    }

    async verifyBuilderCardEmpty(index){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await this.assertVisible(await cards[parseInt(index) - 1]
            .findElement(By.css('[data-testid=\'builder-card--empty\']')));
    }

    async verifyBuilderCardTagSelectNotPresent(index){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await cards[parseInt(index) - 1]
            .findElements(By.css('[data-testid=tag-selector--dropdown-menu--contents]'))
            .then(async elems => {
                expect(await elems.length).to.equal(0);
            });
    }

    async verifyBuilderCardSelectCountNotPresent(index){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await cards[parseInt(index) - 1]
            .findElements(By.css('.tag-selector--count'))
            .then(async elems => {
                expect(await elems.length).to.equal(0);
            });
    }

    async verifyBuilderCardDeleteNotPresent(index){
        let cards = await this.cellOverlay.getTMBuilderCards();
        await cards[parseInt(index) - 1]
            .findElements(By.css('.builder-card--delete'))
            .then(async elems => {
                expect(await elems.length).to.equal(0);
            });
    }

    async closeAllTMQBCards(){
        let cards = await this.cellOverlay.getTMBuilderCards();
        for(let i = cards.length - 1; i > 0; i--){ //close all but last card
            await this.clickAndWait(await cards[i].findElement(By.css('.builder-card--delete')));
        }
    }

    async deselectAllActiveTagsInTMQBCard(index){
        let cards = await this.cellOverlay.getTMBuilderCards();
        let selecteds = await cards[parseInt(index)-1].findElements(By.css('.selector-list--checkbox.selected'));
        for(let i = 0; i < selecteds.length; i++){
            await this.clickAndWait(selecteds[i]);
        }
    }

    async verifyTMQueryBuilderFunctionDuration(duration){
        await this.verifyInputEqualsValue(await this.cellOverlay.getTMBuilderCardMenuDurationInput(), duration);
    }

    async clickCustomFunctionGroup(){
        await this.clickAndWait(await this.cellOverlay.getTMQBCustomFunctionGroup());
    }

    async clickTMQueryBuilderFunctionDuration(){
        await this.clickAndWait(await this.cellOverlay.getTMBuilderCardMenuDurationInput());
    }

    async clickTMQueryBuilderCustomDuration(){
        await this.clickAndWait(await this.cellOverlay.getTMQBCustomWindowPeriod());
    }

    async clickTMQueryBuilderAutoDuration(){
        await this.clickAndWait(await this.cellOverlay.getTMQBAutoWindowPeriod());
    }

    async verifyTMQBFunctionDurationSuggestionCount(count){
        await this.cellOverlay.getTMQBDurationSuggestions().then(async elems => {
            expect(await elems.length).to.equal(parseInt(count));
        });
    }

    async verifyTMQBFunctionDurationSuggestionItems(items){
        let list = items.split(',');
        for (let i = 0; i < list.length; i++){
            let elem = await this.cellOverlay.getTMQBDurationSuggestionByName(list[i].trim());
            await this.scrollElementIntoView(elem);
            await this.assertVisible(elem);
        }
    }

    async clickTMQBFunctionDurationSuggestionItem(item){
        let elem = await this.cellOverlay.getTMQBDurationSuggestionByName(item.trim());
        await this.scrollElementIntoView(elem);
        await this.clickAndWait(elem);
    }

    async verifyTMQueryBuilderFunctionListItems(items){
        let list = items.split(',');
        for(let i = 0; i < list.length; i++){
            let elem = await this.cellOverlay.getTMBuilderCardMenuFunctionListItem(list[i].trim());
            await this.scrollElementIntoView(elem);
            await this.assertVisible(elem);
        }

    }

    async filterQueryBuilderFunctionList(term){
        await this.typeTextAndWait(await this.cellOverlay.getTMBuilderCardMenuFunctionFilter(), term);
    }

    async clearQueryBuilderFunctionListFilter(){
        await this.clearInputText(await this.cellOverlay.getTMBuilderCardMenuFunctionFilter());
    }

    async verifyQuerBuilderFunctionListItemCount(count){
        await this.cellOverlay.getTMBuilderCardMenuFunctionListItems().then(async elems => {
            await expect(await elems.length).to.equal(parseInt(count));
        });
    }

    async getTMPreviewMetrics(){
        /*await this.dbdPage.getCellByName(name).then(async cell => {
            await cell.getRect().then(async rect => {
                // console.log("DEBUG rect for " + name + " " + JSON.stringify(rect))
                if(typeof __dataBuffer.rect === 'undefined'){
                    __dataBuffer.rect = [];
                }
                __dataBuffer.rect[name] = rect;
                //debug why resize not saved
                //await influxUtils.signInAxios('admin');
                //let dashboards = await influxUtils.getDashboards();
                //console.log("DEBUG dashboards " + JSON.stringify(dashboards));

            })
        })*/

        await this.cellOverlay.getTMTop().then(async cell => {
            // TODO generalize the following into system wide method
            await cell.getRect().then(async rect => {
                if(typeof __dataBuffer.rect === 'undefined'){
                    __dataBuffer.rect = [];
                }
                __dataBuffer.rect['TMTop'] = rect;

            });
        });
    }

    async getTMPQueryAreaMetrics(){
        await this.cellOverlay.getTMBottom().then(async cell => {
            await cell.getRect().then(async rect => {
                if(typeof __dataBuffer.rect === 'undefined'){
                    __dataBuffer.rect = [];
                }
                __dataBuffer.rect['TMBottom'] = rect;

            });
        });
    }

    async getTMPreviewCanvas(){
        await this.cellOverlay.getGraphCanvas().then(async canvas => {
            if(typeof __dataBuffer.graphCellLine === 'undefined') {
                __dataBuffer.graphCellLine = [];
            }
            /* eslint-disable require-atomic-updates */
            __dataBuffer.graphCellLine['TMPreview'] = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', canvas);
        });
    }

    async getTMPreviewCanvasAxes(){
        await this.cellOverlay.getGraphCanvasAxes().then(async axes => {
            if(typeof __dataBuffer.graphCellAxes === 'undefined') {
                __dataBuffer.graphCellAxes = [];
            }
            __dataBuffer.graphCellAxes['TMPreviewAxes'] = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', axes);
        });
    }

    async resizeTMPreviewBy(deltaSize){

        await this.cellOverlay.getTMResizerHandle().then(async resizer => {



            let action = await this.driver.actions();
            let action2 = await this.driver.actions();
            await action  //troubleshoot why action occasionally fails
                .move({x:0, y: 0, origin: resizer, duration: 500})
                .press().perform();
            await this.driver.sleep(1000); //wait for animation
            await action2.move({x: 0, y: parseInt(deltaSize.dh), origin: resizer, duration: 500})
                .release()
                .perform();
            await this.driver.sleep(1000); //slight wait for animation

        });
    }

    //todo - generalize this for different elements
    async verifyTMPreviewAreaSizeChange(deltaSize){

        let tolerance = 10;
        await this.cellOverlay.getTMTop().then(async preview => {
            let rect = await preview.getRect();
            //console.log("DEBUG rect " + JSON.stringify(rect))
            let width = parseInt(rect.width);
            let height = parseInt(rect.height);
            let dw = parseInt(deltaSize.dw);
            let dh = parseInt(deltaSize.dh);
            let exph = parseInt(__dataBuffer.rect['TMTop'].height) + dh;
            let expw = parseInt(__dataBuffer.rect['TMTop'].width) + dw;
            expect(Math.abs(exph - height )).to.be.below(tolerance);
            expect(Math.abs(expw - width )).to.be.below(tolerance);

        });

    }

    //todo - generalize this for different elements
    async verifyTMQBAreaSizeChange(deltaSize){
        let tolerance = 10;
        await this.cellOverlay.getTMBottom().then(async preview => {
            let rect = await preview.getRect();
            //console.log("DEBUG rect " + JSON.stringify(rect))
            let width = parseInt(rect.width);
            let height = parseInt(rect.height);
            let dw = parseInt(deltaSize.dw);
            let dh = parseInt(deltaSize.dh);
            let exph = parseInt(__dataBuffer.rect['TMBottom'].height) + dh;
            let expw = parseInt(__dataBuffer.rect['TMBottom'].width) + dw;
            expect(Math.abs(exph - height )).to.be.below(tolerance);
            expect(Math.abs(expw - width )).to.be.below(tolerance);

        });

    }

    async verifyTMPreviewCanvasChange(){
        await this.driver.sleep(3000); //troubleshoot why no change
        await this.cellOverlay.getGraphCanvas().then(async canvasLine => {
            let currentLine = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasLine);
/*
            await fs.writeFile('before.png',  __dataBuffer.graphCellLine['TMPreview'].split(',')[1], 'base64', (err) => {
                if (err) {
                    console.log(err);
                }
            });


            await fs.writeFile('after.png', currentLine.split(',')[1], 'base64', (err) => {
                if (err) {
                    console.log(err);
                }
            });
*/
            await expect(currentLine).to.not.equal(__dataBuffer.graphCellLine['TMPreview']);
        });
    }

    async verifyTMPreviewCanvasNoChange(){
        await this.cellOverlay.getGraphCanvas().then(async canvasLine => {
            let currentLine = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasLine);

            await expect(currentLine).to.equal(__dataBuffer.graphCellLine['TMPreview']);
        });

    }

    async verifyTMPreviewAxesChange(){
        await this.cellOverlay.getGraphCanvasAxes().then(async canvasAxes => {
            let currentAxes = await this.driver
                .executeScript('return arguments[0].toDataURL(\'image/png\');', canvasAxes);

            await expect(currentAxes).to.not.equal(__dataBuffer.graphCellAxes['TMPreviewAxes']);
        });
    }

    async verifyTMPreviewCanvasNotPresent(){
        await this.assertNotPresent(cellEditOverlay.getGraphCanvasSelector());
    }

    async verifyTMPreviewCanvasAxesNotPresent(){
        await this.assertNotPresent(cellEditOverlay.getGraphCanvasAxesSelector());
    }

    async clickTMAddQuery(){
        await this.clickAndWait(await this.cellOverlay.getTMBuilderTabsAddQuery());
    }

    async verifyTMQueryBucketSelected(bucket){
        await this.verifyElementContainsText(await this.cellOverlay.getTMQBSelectedBucket(), bucket);

    }

    async verifyTMQueryCardSelected(index,tag){
        await this.verifyElementContainsText(await this.cellOverlay
            .getTMQBSelectedTagOfCard(parseInt(index) - 1), tag);
    }

    async verifyTMQueryFunctionsSelected(funcs){
        let list = funcs.split(',');
        for(let i = 0; i < list.length; i++){
            await this.cellOverlay.getTMQBSelectedFunctionByName(list[i].trim()).then(async elems => {
                expect(elems.length).to.equal(1, ` selected function ${list[i]} should occur once` );
            });
        }

    }

    async clickTMQBFunction(func){
        await this.clickAndWait(await this.cellOverlay.getTMBuilderCardMenuFunctionListItem(func));
    }

    async verifyTMQBActiveQuery(name){
        await this.cellOverlay.getTMQBActiveQueryTab().then(async tab => {
            await tab.findElement(By.css('.query-tab--name')).then(async elem => {
                this.verifyElementContainsText(elem, name);
            });
        });
    }

    async clickOnTMQBQueryTab(title){
        await this.clickAndWait(await this.cellOverlay.getTMQBQueryTabByName(title));
    }

    async rightClickTMQBQueryTabTitle(title){
        await this.cellOverlay.getTMQBQueryTabByName(title).then(async elem => {
            let action = this.driver.actions();

            await action.contextClick(elem).perform();
        });
    }

    async clickTMQBQueryTabRightClickMenuItem(item){
        await this.clickAndWait(await this.cellOverlay.getTMQBRightClickItem(item));
    }

    async enterNewTMQBQueryTabName(name){
        await this.typeTextAndWait(await this.cellOverlay.getTMQBQueryTabNameInput(),
            name + Key.ENTER);
    }

    async verifyNoTMQBQueryTabNamed(name){
        await this.assertNotPresent(cellEditOverlay.getTMQBQueryTabSelectorByName(name));
    }

    async clickRightTMQBQuery(name){

        await this.clickRAndWait(await this.cellOverlay.getTMQBQueryTabByName(name));

        /*
        await this.cellOverlay.getTMQBQueryTabByName(name).then(async tab => {
            await this.clickRAndWait(tab);
        });
        */
    }

    async clickTMQBHideQuery(name){
        await this.cellOverlay.getTMQBQueryTabByName(name).then(async tab => {
            await this.clickAndWait(await tab.findElement(By.css('.query-tab--hide')));
        });
    }

    async clickTMQBDeleteQuery(name){
        await this.cellOverlay.getTMQBQueryTabByName(name).then(async tab => {
            await this.clickAndWait(await tab.findElement(By.css('.query-tab--close')));
        });
    }

    async verifyTMQBNumberOfQueryTabs(count){
        await this.cellOverlay.getTMQBQueryTabs().then(async elems => {
            await expect(elems.length).to.equal(parseInt(count),
                `Expected number of query tabs to equals ${count}`);
        });
    }

    async verifyTMQBScriptEditorContents(script){
        let text = await this.getMonacoEditorText();
        await expect(text.trim()).to.equal(script.trim());
    }

    async updateTMQBScriptEditorContents(script){
        await this.clearMonacoEditorText(await this.cellOverlay.getScriptMonacoEditor());
        await this.setMonacoEditorText(await this.cellOverlay.getScriptMonacoEditor(), script);
    }

    async verifyTMEmptyGraphErrorMessage(msg){
        await this.verifyElementContainsText(await this.cellOverlay.getTMEmptyGraphErrMessage(), msg);
    }

    async verifyTMQEFunctionCategoriesDisplayed(cats){
        let catList = cats.split(',');
        for(let i = 0; i < catList.length; i++){
            await this.scrollElementIntoView(await this.cellOverlay.getTMQEFunctionCategory(catList[i].trim()));
            await this.assertVisible(await this.cellOverlay.getTMQEFunctionCategory(catList[i].trim()));
        }
    }

    async filterTMQEFunctionsList(term){
        await this.typeTextAndWait(await this.cellOverlay.getTMQEFunctionFilter(), term);
    }

    async clearTMQEFunctionsListFilter(){
        await this.clearInputText(await this.cellOverlay.getTMQEFunctionFilter());
    }

    async verifyTMQEVisibleFunctions(funcs){
        let funcList = funcs.split(',');
        for(let i = 0; i < funcList.length; i++){
            await this.scrollElementIntoView(await this.cellOverlay.getTMQEFunctionListItem(funcList[i].trim()));
            await this.assertVisible(await this.cellOverlay.getTMQEFunctionListItem(funcList[i].trim()));
        }
    }

    async verifyTMQENotVisibleFunctions(funcs){
        let funcList = funcs.split(',');
        for(let i = 0; i < funcList.length; i++) {
            await this.assertNotPresent(cellEditOverlay.getTMQEFunctionListItemSelector(funcList[i].trim()));
        }
    }

    async clickTMQEFunction(func){
        await this.scrollElementIntoView(await this.cellOverlay.getTMQEFunctionListItem(func.trim()));
        await this.clickAndWait(await this.cellOverlay.getTMQEFunctionListItem(func.trim()));
    }

    async clickInjectTMQEFunction(func){
        await this.scrollElementIntoView(await this.cellOverlay.getTMQEFunctionListItem(func.trim()));
        await this.hoverOver(await this.cellOverlay.getTMQEFunctionListItem(func.trim()))
        await this.clickAndWait(await this.cellOverlay.getTMQEFunctionListItemInjector(func.trim()));
    }

    async hoverOverTMQEFunction(func){
        await this.scrollElementIntoView(await this.cellOverlay.getTMQEFunctionListItem(func.trim()));
        await this.hoverOver(await this.cellOverlay.getTMQEFunctionListItem(func.trim()));
    }

    async verifyTMQEFunctionPopupDescription(text){
        await this.verifyElementText(await this.cellOverlay.getTMQEFunctionPopupDescription(), text);
    }

    async verifyTMQEFunctionPopupSnippet(text){
        await this.verifyElementText(await this.cellOverlay.getTMQEFunctionPopupSnippet(), text);
    }

    async verifyTMQEFunctionPopupNotVisible(){
        await this.assertNotPresent(cellEditOverlay.getTMQEFunctionPopupSelector());
    }

    async hoverOverTMQETimerangeDropdown(){
        await this.hoverOver(await this.cellOverlay.getTMTimeRangeDropdown());
    }

    async hoverOverTMCellEditSubmit(){
        await this.hoverOver(await this.cellOverlay.getTimemachineSubmit());
    }

    async sendKeysToTimeMachineFluxEditor(keys){
        await this.sendMonacoEditorKeys(await this.cellOverlay.getScriptMonacoEditor(), keys);
    }

    async verifyTMRawDataTableNotPresent(){
        await this.assertNotPresent(cellEditOverlay.getTMRawDataTableSelector());
    }

    async verifyTMRawDataTablePresent(){
        await this.assertVisible(await this.cellOverlay.getTMRawDataTable());
    }

    async clickTMRawDataToggle(){
        await this.clickAndWait(await this.cellOverlay.getTMRawDataToggle());
    }

    async scrollTMRawDataTableHorizontally(dist){

        await this.cellOverlay.getTMRawDataScrollHThumb().then(async scroller => {

            let action = await this.driver.actions();

            await action.move({x: 0, y: 0, origin: scroller, duration: 500})
                .press()
                .move({ x: dist, y: 0, origin: scroller, duration: 500 })
                .perform();
        });

    }

    async scrollTMRawDataTableVertically(dist){

        await this.cellOverlay.getTMRawDataScrollVThumb().then(async scroller => {

            let action = await this.driver.actions();

            await action.move({x: 0, y: 0, origin: scroller, duration: 500})
                .press()
                .move({ x: 0, y: dist, origin: scroller, duration: 500 })
                .perform();

        });

    }

    async calculateTMRawDataVisibleTableRow(){

        let results = [];

        console.log("DEBUG calculateTMRawDataTableRow enter")

        let i = 0;
        let lastLeft = -1;
        let cell = await this.cellOverlay.getTMRawDataCellByIndex(i + 1);
        let currLeft = parseInt(await cell.getCssValue('left'));
        console.log("DEBUG currLeft " + currLeft + " lastLeft " + lastLeft);
        while(currLeft > lastLeft){
            let rec = {};
            rec.width = parseInt(await cell.getCssValue('width'));
            rec.val = await cell.getText();
            results[i] = rec;
            //console.log(`DEBUG results[${i}] ${results[i].width} ${results[i].val}`);
            lastLeft = parseInt(await cell.getCssValue('left'));
            i++;
            cell = await this.cellOverlay.getTMRawDataCellByIndex(i + 1);
            currLeft = parseInt(await cell.getCssValue('left'));
            //console.log("DEBUG currLeft " + currLeft + " lastLeft " + lastLeft);
        }

        console.log("DEBUG calculateTMRawDataTableRow exit")

        return results;

    }

    async verifyTMRawDataCellContents(coords, value){
        let cells = await this.cellOverlay.getTMRawDataCells();
       /* let grid = await this.cellOverlay.getTMRawDataReactGrid();
        let trackH = await this.cellOverlay.getTMRawDataScrollTrackH();
        let trackHWidth = parseInt(await trackH.getCssValue('width'));
        let trackV = await this.cellOverlay.getTMRawDataScrollTrackV();
        let strdH = parseInt(await cells[0].getCssValue('height'));
        let typW = parseInt(await cells[0].getCssValue('width'));
        console.log("DEBUG coords "  + JSON.toString(coords));
        console.log("DEBUG value " + value);
        console.log("DEBUG cells.length " + await cells.length);
        console.log("DEBUG cells[0].top " + await cells[0].getCssValue('top'));
        console.log("DEBUG cells[0].left " + await cells[0].getCssValue('left'));
        console.log("DEBUG strdH " + strdH);
        console.log("DEBUG typW " + typW);
        console.log("DEBUG ReactGrid w: " + await grid.getCssValue('width') +
                           " h: " + await grid.getCssValue('height'));
        console.log("DEBUG trackH.width "  + await trackH.getCssValue('width'));
        console.log("DEBUG trackV.height " + await trackV.getCssValue('height'));

        let rowWidths = await this.calculateTMRawDataVisibleTableRow(cells);
        let rowWidth = 0;
        for(let i = 0; i < rowWidths.length; i++){
            rowWidth += rowWidths[i].width;
        }

        console.log("DEBUG rowWidths " + rowWidths);
        console.log("DEBUG rowWidth " + rowWidth);
        console.log("DEBUG type coords.x " + typeof(coords.x));
        if(coords.x > rowWidths.length){
            console.log("need to scroll horizontally rowWidth type " + typeof(rowWidth) + " trackHWidth type " + typeof(trackHWidth))
            await this.scrollTMRawDataTableHorizontally(rowWidth - trackHWidth);
        }

        */

        let rowWidths = await this.calculateTMRawDataVisibleTableRow(cells);
        let cellIndex = 1 + coords.x + coords.y * (rowWidths.length); //N.B. nth-of starts with index of 1
       console.log("DEBUG cellIndex " + cellIndex);
       let targetCell = await this.cellOverlay.getTMRawDataCellByIndex(cellIndex);
       console.log("DEBUG cell text " + await targetCell.getText());
       await this.assertVisible(targetCell); //should not be visible after scroll

        //Find column location by left
        //Find next row when left:0 or left:lower than previous cell
        //move right or left using scroll below - use width of first cell as guied (2 * width)
       // await this.scrollTMRawDataTableHorizontally(typW);
       // await this.scrollTMRawDataTableVertically(strdH);
        //scroll down based on height - which should be standard
        //use top / height to determine row
    }

    async scrollTMRawDataTable(coords){
        if(coords.x !== 0){
            await this.scrollTMRawDataTableHorizontally(coords.x);
        }

        if(coords.y !== 0){
            await this.scrollTMRawDataTableVertically(coords.y);
        }
    }

    async clickTMDownloadCSV(){
        await this.clickAndWait(await this.cellOverlay.getTMDownloadCSV(),
            async () => { await this.driver.sleep(2000) }); //todo better wait - 2 sec to download
        await influxUtils.dumpDownloadDir();
    }

    async clickTMQEVariablesTab(){
        await this.clickAndWait(await this.cellOverlay.getTMQEVariablesTab());
    }

    async verifyTMQEVariablesList(varList){
        let list = varList.split(',');
        for(let i = 0; i < list.length; i++){
            await this.assertVisible(await this.cellOverlay.getTMQEVariablesLabel(list[i].trim()));
        }
    }

    async verifyTMQWVarieblesListAbsent(varList){
        let list = varList.split(',');
        for(let i = 0; i < list.length; i++){
            await this.assertNotPresent(await cellEditOverlay.getTMQEVariablesLabelSelector(list[i].trim()));
        }
    }

    async enterTMQEVariableFilterValue(value){
        await this.typeTextAndWait(await this.cellOverlay.getTMQEVariablesFilter(), value)
    }

    async clearTMQEVariablesFilter(){
        await this.clearInputText(await this.cellOverlay.getTMQEVariablesFilter());
    }

    async hoverOverTMQEVariable(varname){
        await this.hoverOver(await this.cellOverlay.getTMQEVariablesLabel(varname.trim()));
    }

    async clickTMQEVariable(varname){
        await this.clickAndWait(await this.cellOverlay.getTMQEVariablesLabel(varname.trim()));
    }

    async clickInjectTMQEVariable(varname){
        await this.hoverOver(await this.cellOverlay.getTMQEVariablesLabel(varname.trim()));
        await this.clickAndWait(await this.cellOverlay.getTMQEVariablesInject(varname.trim()));
    }

    async verifyTMQEVariablePopoverNotVisible(){
        await this.assertNotPresent(await cellEditOverlay.getTMQEVariablesPopoverSelector())
    }

    async verifyTMQEVariablePopoverVisible(){
        await this.assertVisible(await this.cellOverlay.getTMQEVariablesPopover());
    }

    async clickTMQEVariablePopoverVarDropdown(){

        await this.driver.executeScript('arguments[0].style.border=\'3px solid red\'',
            await this.cellOverlay.getTMQEVariablesPopoverContents());

        await this.hoverOver(await this.cellOverlay.getTMQEVariablesPopoverContents());

        await this.driver.sleep(500);
/*
        await this.driver.executeScript('arguments[0].style.border=\'3px solid red\'',
            await this.cellOverlay.getTMQEVariablePopoverDropdown());

        let elem = await this.cellOverlay.getTMQEVariablesLabel('KARTA');

        let dd = await this.cellOverlay.getTMQEVariablePopoverDropdown();
        console.log("DEBUG dd " + JSON.stringify(await dd.getRect()));
        console.log("DEBUG elem " + JSON.stringify(await elem.getRect()));
        console.log("DEBUG win " + JSON.stringify(await this.driver.manage().window().getRect()));
        //await this.driver.sleep(1);
        //let action = await this.driver.actions();
        //await action.move({x: -100, y:5, origin: Origin.POINTER,  duration: 500})
        //    .perform();

        //await dd.click();

        //await this.driver.executeScript('arguments[0].click()',
        //    await this.cellOverlay.getTMQEVariablePopoverDropdown());

        await this.driver.sleep(3000); */
        // for some reason standard methods cause popover to disappear
        //await this.hoverOver(await this.cellOverlay.getTMQEVariablePopoverDropdown());
        //await this.clickAndWait(await this.cellOverlay.getTMQEVariablePopoverDropdown());
    }

}

module.exports = cellOverlaySteps;
