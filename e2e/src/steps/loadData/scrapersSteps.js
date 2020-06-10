const { By, Key } = require('selenium-webdriver');
const { expect } = require('chai');

const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const scrapersTab = require(__srcdir + '/pages/loadData/scrapersTab.js');
const influxUtils = require(__srcdir + '/utils/influxUtils.js');

let namedQueriesMap = new Map();
namedQueriesMap.set('Measurements', 'from(bucket: "[BUCKET]")\n' +
    '    |> range(start: -1h, stop: now())\n' +
    '    |> filter(fn: (r) => true)\n' +
    '    |> keep(columns: ["_measurement"])\n' +
    '    |> group()\n' +
    '    |> distinct(column: "_measurement")\n' +
    '    |> limit(n: 200)\n' +
    '    |> sort()');

class scrapersSteps extends baseSteps{

    constructor(driver){
        super(driver);
        this.scrapeTab = new scrapersTab(driver);
    }

    async isLoaded(){
        await this.scrapeTab.isTabLoaded();
    }

    async verifyExistsCardByName(card){
        await this.assertVisible(await this.scrapeTab.getScraperCardByName(card));
    }

    async verifyScraperCardHasBucket(scraper, bucketName){
        await this.driver.findElement(
            By.xpath(`//*[@data-testid='resource-card'][.//span[text()='${scraper}']]//*[@data-testid='resource-list--meta']//*[contains(text(),'Bucket')]`))
            .then(async elem => {
                await elem.getText().then(async elText => {
                    await expect(elText).to.include(bucketName);
                });
            });
    }

    async verifyScraperCardHasEndpoint(scraper, endpoint){
        await this.driver.findElement(
            By.xpath(`//*[@data-testid='resource-card'][.//span[text()='${scraper}']]//*[@data-testid='resource-list--meta']//*[contains(text(),'URL')]`))
            .then(async elem => {
                await elem.getText().then(async elText => {
                    await expect(elText).to.include(endpoint);
                });
            });
    }

    async verifyScrapersTabIsLoaded(){
        await this.scrapeTab.isTabLoaded();
        await this.assertVisible(await this.scrapeTab.getCreateScraperHeader());
        await this.assertVisible(await this.scrapeTab.getScrapersFilter());
        await this.assertVisible(await this.scrapeTab.getSortTypeButton());
        //await this.assertVisible(await this.scrapeTab.getNameSort());
        //await this.assertVisible(await this.scrapeTab.getUrlSort());
        //await this.assertVisible(await this.scrapeTab.getBucketSort());
    }

    async clickCreateScraperButtonEmpty(){
        await this.scrapeTab.getCreateScraperEmpty().then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); //todo implement better wait;
            });
        });
    }

    async clickCreateScraperButtonInHeader(){
        await this.scrapeTab.getCreateScraperHeader().then(async elem => {
            await elem.click().then(async () => {
                await this.driver.sleep(100); //todo implement better wait;
            });
        });
    }

    async verifyCreateScraperEmptyNotPresent(){
        await this.assertNotPresent(scrapersTab.getCreateScraperEmptySelector());
    }

    async verifyScrapersSortOrderByName(items){
        let itemsArray = items.split(',');
        await this.scrapeTab.getScraperCards().then(async cards => {
            for( let i = 0; i < cards.length; i++){
                let cardName = await cards[i].findElement(By.xpath('.//span[contains(@class,\'cf-resource-name--text\')]/span'));
                let cardText = await cardName.getText();
                expect(cardText).to.equal(itemsArray[i]);
            }
        });
    }

    async enterScrapersFilterValue(value){
        await this.scrapeTab.getScrapersFilter().then(async filter => {
            await filter.clear().then(async () => {
                await filter.sendKeys(value).then(async () => {
                    await this.driver.sleep(100); //todo implement better wait
                });
            });
        });
    }

    async clearScraperFilter(){
        await this.scrapeTab.getScrapersFilter().then(async filter => {
            await filter.clear().then(async () => {
                await this.driver.sleep(100); //todo implement better wait
            });
        });
    }

    async verifyScraperCardNotPresent(scraper){
        await this.assertNotPresent({type: 'xpath', selector: `//*[@data-testid='resource-card']//span[text()='${scraper}']`});
    }

    async clickScraperNameSortButton(){
        await this.scrapeTab.getNameSort().then(async button => {
            await button.click().then(async () => {
                await this.driver.sleep(100); //todo implement better wait
            });
        });
    }

    async clickScraperURLSortButton(){
        await this.scrapeTab.getUrlSort().then(async button => {
            await button.click().then(async () => {
                await this.driver.sleep(100); //todo implement better wait
            });
        });
    }

    async clickScraperBucketSortButton(){
        await this.scrapeTab.getBucketSort().then(async button => {
            await button.click().then(async() => {
                await this.driver.sleep(100);
            });
        });
    }

    async mouseOverScraperCardName(name){
        await this.hoverOver(await this.scrapeTab.getScraperCardName(name));
    }

    async clickScraperCardNameEditButton(name){
        await this.scrapeTab.getScraperCardNameEditButton(name).then(async button => {
            await button.click().then(async () => {
                await this.driver.sleep(100); // todo implement better wait
            });
        });
    }

    async enterNewScraperName(newName, oldName){
        await this.scrapeTab.getScraperCardNameEditField(oldName).then(async input => {
            await input.sendKeys(newName + Key.ENTER).then(async () => {
                await this.driver.sleep(100); //todo implement better wait
            });
        });
    }

    async verifyNamedQueryResponseValues(queryName, userName, bucketName, values, field = '_value'){
        let user = await influxUtils.getUser(userName);
        let query;
        query = namedQueriesMap.get(queryName);
        query = query.replace('[BUCKET]', (bucketName === 'DEFAULT') ? user.bucket : bucketName);

        let results = await influxUtils.query(user.username, query);

        let targetValues = values.split(',');

        targetValues.forEach(async value => {
            await expect(results.filter(rec =>
                rec[field] === targetValues[0]
            ).length).to.be.above(0, `failed to locate record with value ${value} in field ${field}`);
        });
    }

    async verifyScraperCardDeleteNotPresent(name){
        await this.scrapeTab.getScrapersFilter().then(async elem => {
            await elem.click().then(async () => {  //remove focus from list
                await this.driver.sleep(500); //todo fix later - losing patience - better wait
                await this.assertNotVisible(await this.scrapeTab.getScraperCardDeleteByName(name));
            });
        });

    }

    async hoverOverScraperCard(name){
        await this.hoverOver(await this.scrapeTab.getScraperCardByName(name));
    }

    async clickScraperCardDeleteButton(name){
        await this.scrapeTab.getScraperCardDeleteByName(name).then(async button => {
            await button.click().then(async () => {
                await this.driver.sleep(100); // todo better wait
            });
        });
    }

    async clickScraperCardDeleteConfirm(name){
        await this.scrapeTab.getScraperCardDeleteConfirmByName(name).then(async button => {
            await button.click().then(async () => {
                await this.driver.sleep(200); //todo better wait
            });
        });
    }



}

module.exports = scrapersSteps;
