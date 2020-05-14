const { expect } = require('chai');

const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const labelsTab = require(__srcdir + '/pages/settings/labelsTab.js');

let colorMap = new Map([
    ['thunder', '#FFD255'],
    ['topaz', '#E85B1C']
]);

class labelsSteps extends baseSteps{

    constructor(driver){
        super(driver);
        this.labTab = new labelsTab(driver);
    }

    async isLoaded(){
        await this.labTab.isTabLoaded();
    }

    async verifyCreateLabelPopupLoaded(){
        await this.verifyElementContainsText(await this.labTab.getPopupTitle(), 'Create Label');
        await this.assertVisible(await this.labTab.getPopupDismiss());
        await this.assertVisible(await this.labTab.getLabelPopupNameInput());
        await this.assertVisible(await this.labTab.getLabelPopupDescrInput());
        await this.assertVisible(await this.labTab.getLabelPopupColorPicker());
        await this.assertVisible(await this.labTab.getLabelPopupColorInput());
        await this.assertVisible(await this.labTab.getLabelPopupCreateBtn());
        await this.assertVisible(await this.labTab.getLabelPopupCancelBtn());
    }

    async verifyEditLabelPopuLoaded(){
        await this.verifyElementContainsText(await this.labTab.getPopupTitle(), 'Edit Label');
        await this.assertVisible(await this.labTab.getPopupDismiss());
        await this.assertVisible(await this.labTab.getLabelPopupNameInput());
        await this.assertVisible(await this.labTab.getLabelPopupDescrInput());
        await this.assertVisible(await this.labTab.getLabelPopupColorPicker());
        await this.assertVisible(await this.labTab.getLabelPopupColorInput());
        await this.assertVisible(await this.labTab.getLabelPopupCreateBtn());
        await this.assertVisible(await this.labTab.getLabelPopupCancelBtn());
    }

    async dismissCreateLabelPopup(){
        await this.clickAndWait(await this.labTab.getPopupDismiss());
    }

    async cancelCreateLabelPopup(){
        await this.clickAndWait(await this.labTab.getLabelPopupCancelBtn(),
            async () => { await this.delay(1000);}); //can be slow to remove - todo better wait
    }

    async clickCreateLabelButtonEmpty(){
        await this.clickAndWait(await this.labTab.getCreateLabelEmpty()); //todo better wait
    }

    async clickCreateLabelButtonHeader(){
        await this.clickAndWait(await this.labTab.getCreateLabelHeader()); //todo better wait
    }

    // value is hex value eg '#EAC8A2'
    static hexColorValueToDecimalArray(value){
        value = value.slice(1);
        let values = value.match(/.{1,2}/g);
        for(let i = 0; i < values.length; i++){
            values[i] = parseInt(values[i], 16);
        }
        return values;
    }

    // value needs to follow pattern 'rgba(255,255,255,1)'
    static rgbaColorValueToDecimalArray(value){
        value = value.replace('rgba(', '');
        value = value.replace(')', '');
        let values = value.split(',');
        values.pop();
        for(let i = 0; i < values.length; i++){
            values[i] = parseInt(values[i], 10);
        }
        return values;
    }

    async verifyInputColorMatchesPreview(){
        await this.labTab.getLabelPopupColorInput().then(async input => {
            let values = labelsSteps.hexColorValueToDecimalArray(await input.getAttribute('value'));
            //console.log("DEBUG input color   " + values);
            await this.labTab.getLabelPopupPreview().then(async elem => {
                let bkgcols = await elem.getCssValue('background-color');
                let bkgVals = labelsSteps.rgbaColorValueToDecimalArray(bkgcols);
                for(let i = 0; i < bkgVals.length; i++){
                    await expect(bkgVals[i]).to.equal(values[i]);
                }
            });

        });
    }

    async verifyLabelPopupPreviewTextColor(value){
        let values = labelsSteps.hexColorValueToDecimalArray(value);
        //console.log("DEBUG values " + values);
        await this.labTab.getLabelPopupPreviewPill().then(async elem => {
            let colors = await elem.getCssValue('color');
            let colVals = labelsSteps.rgbaColorValueToDecimalArray(colors);
            for(let i = 0; i < colVals.length; i++){
                await expect(colVals[i]).to.equal(values[i]);
            }
        });
    }

    async verifyCreateLabelPreviewPillText(text){
        await this.verifyElementContainsText(await this.labTab.getLabelPopupPreviewPill(), text);
    }

    async typeCreateLabelPopupName(text){
        await this.typeTextAndWait(await this.labTab.getLabelPopupNameInput(), text);
    }

    async typeCreateLabelPopupDescription(descr){
        await this.typeTextAndWait(await this.labTab.getLabelPopupDescrInput(), descr);
    }


    async clearLabelPopupColorInput(){
        await this.clearInputText(await this.labTab.getLabelPopupColorInput());
    }

    async clearLabelPopupNameInput(){
        await this.clearInputText(await this.labTab.getLabelPopupNameInput());
    }

    async clearLabelPopupDescriptionInput(){
        await this.clearInputText(await this.labTab.getLabelPopupDescrInput());
    }


    async typeCreateLabelPopupColor(value){
        await this.typeTextAndWait(await this.labTab.getLabelPopupColorInput(), value,
            async () => { await this.delay(500); }); //preview can be slow to update - todo better wait
    }

    async clickLabelPopupLuckyColor(){
        await this.clickAndWait(await this.labTab.getLabelPopupRandomColor(),
            async () => { await this.delay(500); }); // preview can be slow to update - todo better wait
    }

    async verifyLabelPopupColorInputValue(value){
        await this.verifyInputEqualsValue(await this.labTab.getLabelPopupColorInput(), value);
    }

    async verifyLabelPopupColorInputNotValue(value){
        await this.verifyInputDoesNotContainValue(await this.labTab.getLabelPopupColorInput(), value);
    }

    async clickLabelPopupColorSwatch(name){
        await this.clickAndWait(await this.labTab.getLabelPopupColorSwatch(name),
            async () => { await this.delay(500); }); // preview can be slow to update - todo better wait
    }

    async clickLabelPopupCreateButton(){
        await this.clickAndWait(await this.labTab.getLabelPopupCreateBtn(),
            async () => { await this.delay(1000); }); //popup can be slow to close
    }

    async verifyLabelCardInList(name){
        await this.assertVisible(await this.labTab.getLabelCard(name));
    }



    async verifyLabelCardPillColor(name, color){
        await this.delay(333); //animation changes can take a split second
        //console.log("DEBUG color " + color)
        if(color[0] !== '#'){
            color = colorMap.get(color.toLowerCase());
            //            console.log("DEBUG color switched " + color)
        }

        let values = labelsSteps.hexColorValueToDecimalArray(color);
        await this.labTab.getLabelCardPill(name).then(async elem => {
            let pillBkgColor = await elem.getCssValue('background-color');
            let pillVals = labelsSteps.rgbaColorValueToDecimalArray(pillBkgColor);
            //          console.log("DEBUG pillVals " + pillVals);
            //          console.log("DEBUG values " + values);
            for(let i = 0; i < pillVals.length; i++){
                expect(pillVals[i]).to.equal(values[i]);
            }
        });
    }

    async verifyLabelCardDescription(name, descr){
        await this.verifyElementContainsText(await this.labTab.getLabelCardDescr(name), descr);
    }

    async clickLabelCardPill(name){
        await this.clickAndWait(await this.labTab.getLabelCardPill(name));
    }

    async verifyLabelSortOrder(labels){
        let lblArr = labels.split(',');
        await this.labTab.getLabelCardPills().then(async pills => {
            for(let i = 0; i < lblArr.length; i++){
                await expect(await pills[i].getText()).to.equal(lblArr[i]);
            }
        });
    }

    async clickLabelSortByName(){
        await this.clickAndWait(await this.labTab.getLabelNameSort());
    }

    async clickLabelSortByDescription(){
        await this.clickAndWait(await this.labTab.getLabelDescSort());
    }

    async clearLabelFilterInput(){
        await this.clearInputText(await this.labTab.getLabelsFilter());
    }

    async enterTextIntoLabelFilter(text){
        await this.typeTextAndWait(await this.labTab.getLabelsFilter(), text);
    }

    async verifyLabelsNotPresent(labels){
        let lblArr = labels.split(',');
        for(let i = 0; i < lblArr.length; i++){
            await this.assertNotPresent(labelsTab.getLabelCardSelector(lblArr[i]));
        }
    }

    async hoverOverLabelCard(name){
        await this.hoverOver(await this.labTab.getLabelCard(name));
    }

    async clickLabelCardDelete(name){
        await this.clickAndWait(await this.labTab.getLabelCardDelete(name));
    }

    async clickLabelCardDeleteConfirm(name){
        await this.clickAndWait(await this.labTab.getLabelCardDeleteConfirm(name));
    }

}

module.exports = labelsSteps;
