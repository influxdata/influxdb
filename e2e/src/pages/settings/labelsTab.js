const { By } = require('selenium-webdriver');

const settingsPage = require(__srcdir + '/pages/settings/settingsPage.js');
const basePage = require(__srcdir + '/pages/basePage.js');

const labelsFilter = '[data-testid=search-widget]';
const createLabelHeader = '[data-testid=button-create]';
const labelNameSort = '[data-testid=sorter--name]';
const labelDescSort = '[data-testid=sorter--desc]';
const createLabelEmpty = '[data-testid=button-create-initial]';
const labelCard = '//*[@data-testid=\'label-card\'][.//span[text()=\'%NAME%\']]';
const labelCardPills = '[data-testid^=label--pill]';
const labelCardPill = '//*[@data-testid=\'label-card\']//div[./span[@data-testid=\'label--pill %NAME%\']]';
const labelCardDescr = '//*[@data-testid=\'label-card\'][.//span[text()=\'%NAME%\']]//*[@data-testid=\'label-card--description\']';
const labelCardDelete = '//*[@data-testid=\'label-card\'][.//span[text()=\'%NAME%\']]//button[@data-testid=\'context-delete-menu\']';
const labelCardDeleteConfirm = '//*[@data-testid=\'label-card\'][.//span[text()=\'%NAME%\']]//button[@data-testid=\'context-delete-label\']';

const urlCtx = 'labels';

//Create Label Popup
const labelPopupNameInput = '[data-testid=create-label-form--name]';
const labelPopupDescrInput = '[data-testid=create-label-form--description]';
const labelPopupColorPicker = '[data-testid=color-picker]';
const labelPopupColorInput = '[data-testid=color-picker--input]';
const labelPopupCreateBtn = '[data-testid=create-label-form--submit]';
const labelPopupCancelBtn = '[data-testid=create-label-form--cancel]';
const labelPopupPreview = '[data-testid=overlay--body] [data-testid=form--box] div';
const labelPopupPreviewPill = '[data-testid=overlay--body] [data-testid^=label--pill]';
const labelPopupRandomColor = '[data-testid=color-picker--randomize]';
const labelPopupColorSwatch = '[data-testid=color-picker--swatch][title=\'%NAME%\']';

class labelsTab extends settingsPage{

    constructor(driver){
        super(driver);
    }

    async isTabLoaded(){
        await super.isTabLoaded(urlCtx,
            [
                {type: 'css', selector: labelsFilter},
                {type: 'css', selector: createLabelHeader},
                basePage.getSortTypeButtonSelector()
                //{type: 'css', selector: labelNameSort},
                //{type: 'css', selector: labelDescSort},
            ]
        );
    }

    async getLabelNameSort(){
        return await this.driver.findElement(By.css(labelNameSort));
    }

    async getLabelDescSort(){
        return await this.driver.findElement(By.css(labelDescSort));
    }

    async getLabelsFilter(){
        return await this.driver.findElement(By.css(labelsFilter));
    }

    async getCreateLabelHeader(){
        return await this.driver.findElement(By.css(createLabelHeader));
    }

    async getCreateLabelEmpty(){
        return await this.driver.findElement(By.css(createLabelEmpty));
    }

    async getLabelPopupNameInput(){
        return await this.driver.findElement(By.css(labelPopupNameInput));
    }

    async getLabelPopupDescrInput(){
        return await this.driver.findElement(By.css(labelPopupDescrInput));
    }

    async getLabelPopupColorPicker(){
        return await this.driver.findElement(By.css(labelPopupColorPicker));
    }

    async getLabelPopupColorInput(){
        return await this.driver.findElement(By.css(labelPopupColorInput));
    }

    async getLabelPopupCreateBtn(){
        return await this.driver.findElement(By.css(labelPopupCreateBtn));
    }

    async getLabelPopupCancelBtn(){
        return await this.driver.findElement(By.css(labelPopupCancelBtn));
    }

    async getLabelPopupPreview(){
        return await this.driver.findElement(By.css(labelPopupPreview));
    }

    async getLabelPopupPreviewPill(){
        return await this.driver.findElement(By.css(labelPopupPreviewPill));
    }

    async getLabelPopupRandomColor(){
        return await this.driver.findElement(By.css(labelPopupRandomColor));
    }

    async getLabelPopupColorSwatch(name){
        return await this.driver.findElement(By.css(labelPopupColorSwatch.replace('%NAME%', name)));
    }

    async getLabelCard(name){
        return await this.driver.findElement(By.xpath(labelCard.replace('%NAME%', name)));
    }

    static getLabelCardSelector(name){
        return { type: 'xpath', selector: labelCard.replace('%NAME%', name)};
    }

    async getLabelCardPill(name){
        return await this.driver.findElement(By.xpath(labelCardPill.replace('%NAME%', name)));
    }

    async getLabelCardPills(){
        return await this.driver.findElements(By.css(labelCardPills));
    }

    async getLabelCardDescr(name){
        return await this.driver.findElement(By.xpath(labelCardDescr.replace('%NAME%', name)));
    }

    async getLabelCardDelete(name){
        return await this.driver.findElement(By.xpath(labelCardDelete.replace('%NAME%', name)));
    }

    async getLabelCardDeleteConfirm(name){
        return await this.driver.findElement(By.xpath(labelCardDeleteConfirm.replace('%NAME%', name)));
    }

}

module.exports = labelsTab;
