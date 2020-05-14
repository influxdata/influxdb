const expect = require('chai').expect;
const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const loadDataPage = require(__srcdir + '/pages/loadData/loadDataPage.js');

const { Key } = require('selenium-webdriver');


class loadDataSteps extends influxSteps{

    constructor(driver){
        super(driver);
        this.ldPage = new loadDataPage(__wdriver);
    }

    async isLoaded(){
        await this.ldPage.isLoaded();
    }

    async verifyIsLoaded(){
        this.assertVisible(await this.ldPage.getTabByName('Buckets'));
        this.assertVisible(await this.ldPage.getTabByName('Telegraf'));
        this.assertVisible(await this.ldPage.getTabByName('Scrapers'));
    }

    async clickTab(name){
        await this.ldPage.getTabByName(name).then(async tab => {
            await tab.click();
        });
    }

    async createScraperPopupLoaded(){
        await this.assertVisible(await this.ldPage.getCreateScraperTitle());
        await this.assertVisible(await this.ldPage.getCreateScraperDismiss());
        await this.assertVisible(await this.ldPage.getCreateScraperNameInput());
        await this.assertVisible(await this.ldPage.getCreateScraperBucketDropdown());
        await this.assertVisible(await this.ldPage.getCreateScraperUrlInput());
        await this.assertVisible(await this.ldPage.getCreateScraperCancel());
        await this.assertVisible(await this.ldPage.getCreateScraperSubmit());
    }

    async dismissCreateScraperPopup(){
        await this.ldPage.getCreateScraperDismiss().then(async btn => {
            btn.click().then(async () => {
                await this.driver.sleep(500); //todo better wait
            });
        });
    }

    async verifyCreateScraperPopupNotPresent(){
        await this.assertNotPresent(loadDataPage.getCreateScraperTitleSelector());
        await this.assertNotPresent(loadDataPage.getCreateScraperNameInputSelector());
        await this.assertNotPresent(loadDataPage.getCreateScraperUrlInputSelector());
    }

    async cancelCreateScraperPopup(){
        await this.ldPage.getCreateScraperCancel().then(async btn => {
            await btn.click().then(async () => {
                await this.driver.sleep(500); //todo better wait
            });
        });
    }

    async clearCreateScraperNameInput(){
        await this.ldPage.getCreateScraperNameInput().then(async input => {
            await input.sendKeys(Key.END);
            while((await input.getAttribute('value')).length > 0){
                await input.sendKeys(Key.BACK_SPACE);
            }
            await this.driver.sleep(200);
            //N.B. clear skips hook that triggers messages and other state changes
            //await input.clear().then( async () => {
            //    await this.driver.sleep(500); // todo better wait
            //})
        });
    }

    async clearCreateScraperUrlInput(){
        await this.ldPage.getCreateScraperUrlInput().then(async input => {
            await input.sendKeys(Key.END);
            while((await input.getAttribute('value')).length > 0){
                await input.sendKeys(Key.BACK_SPACE);
            }
            await this.driver.sleep(200);
        });
    }

    async verifyCreateScraperSubmitEnabled(state = true){
        await this.ldPage.getCreateScraperSubmit().then(async elem => {
            expect(await elem.isEnabled()).to.equal(state);
        });
    }

    async enterCreateScraperName(name){
        await this.ldPage.getCreateScraperNameInput().then(async elem => {
            await this.clearInputText(elem).then(async () => {
            //await elem.clear().then(async () => {
                await elem.sendKeys(name).then(async () => {
                    await this.driver.sleep(200); // todo better wait
                });
            });
        });
    }

    async enterCreateScraperTargetURL(url){
        await this.ldPage.getCreateScraperUrlInput().then(async elem => {
            await elem.sendKeys(url).then(async () => {
                await this.driver.sleep(200); // todo better wait
            });
        });
    }

    async clickCreateScraperBucketsDropdown(){
        await this.ldPage.getCreateScraperBucketDropdown().then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); // todo better wait
            });
        });
    }

    async selectCreateScraperBucketsItem(item){
        await this.ldPage.getCreateScraperBucketDropdownItem(item).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); //todo better wait
            });
        });
    }

    async verifyCreateScraperBucketsDropdownItem(item){
        await this.ldPage.getCreateScraperBucketDropdownItem(item).then(async elem => {
            await expect(await elem.isDisplayed()).to.equal(true);
        });
    }

    async verifyNoBucketItemsInBucketsDropdownShown(){
        await this.assertNotPresent(loadDataPage.getCreateScraperBucketDropdownItemsSelector());
    }

    async clickCreateScraperBucketCreateButton(){
        await this.ldPage.getCreateScraperSubmit().then(async btn => {
            await btn.click().then(async () => {
                await this.driver.sleep(300); // todo better wait
            });
        });
    }

    async verifyCreateTelegrafWizardLoaded(){
        await this.assertVisible(await this.ldPage.getPopupWizardTitle());
        await this.assertVisible(await this.ldPage.getPopupWizardSubTitle());
        await this.assertVisible(await this.ldPage.getPopupWizardContinue());
        await this.assertVisible(await this.ldPage.getBucketDropdown());
        await this.assertVisible(await this.ldPage.getPluginsFilter());
    }

    async clickBucketsDropdown(){
        await this.ldPage.getBucketDropdown().then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); // todo better wait
            });
        });
    }

    async selectBucketsDropdownItem(item){
        await this.ldPage.getBucketDropdownItem(item).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); // todo better wait
            });
        });
    }

    async selectTelegrafWizardPluginTile(tile){
        await this.ldPage.getPluginTileByName(tile).then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); //todo better wait
            });
        });
    }

    async enterTelegrafWizardName(name){
        await this.ldPage.getTelegrafNameInput().then(async elem => {
            await this.clearInputText(elem).then(async () => {
                await elem.sendKeys(name).then(async () => {
                    await this.driver.sleep(100); //todo better wait
                });
            });
        });
    }

    async enterTelegrafWizardDescr(descr){
        await this.ldPage.getTelegrafDescrInput().then(async elem => {
            await elem.clear().then(async () => {
                await elem.sendKeys(descr).then(async () => {
                    await this.driver.sleep(100); //todo better wait
                });
            });
        });
    }

}

module.exports = loadDataSteps;
