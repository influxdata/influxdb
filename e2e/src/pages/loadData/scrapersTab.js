const { By } = require('selenium-webdriver');
const loadDataPage = require(__srcdir + '/pages/loadData/loadDataPage.js');
const basePage = require(__srcdir + '/pages/basePage.js');

const scrapersFilter = '[data-testid=search-widget]';
const createScraperHeader = '[data-testid=create-scraper-button-header]';
const nameSort = '[data-testid=resource-list--sorter]:nth-of-type(1)';
const urlSort = '[data-testid=resource-list--sorter]:nth-of-type(2)';
const bucketSort = '[data-testid=resource-list--sorter]:nth-of-type(3)';
const createScraperEmpty = '[data-testid=create-scraper-button-empty]';
const scraperCards = '[data-testid=resource-card]';

const urlCtx = 'scrapers';

class scrapersTab extends loadDataPage{

    constructor(driver){
        super(driver);
    }

    async isTabLoaded(){
        await super.isTabLoaded(urlCtx,
            [
                {type: 'css', selector: scrapersFilter},
                {type: 'css', selector: createScraperHeader},
                basePage.getSortTypeButtonSelector()
                //{type: 'css', selector: nameSort},
                //{type: 'css', selector: urlSort},
                //{type: 'css', selector: bucketSort},
            ]
        );
    }

    async getScraperCardByName(name){
        return await this.driver.findElement(By.xpath(`//*[@data-testid='resource-card'][//span[text() = '${name}']]`));
    }

    async getScraperCardName(name){
        return await this.driver.findElement(By.xpath(`//*[@data-testid='resource-editable-name']//span[text()='${name}']`));
    }

    async getScraperCardNameEditButton(name){
        return await this.driver.findElement(By.xpath(`//*[./*[@data-testid='resource-editable-name'][.//span[text()='${name}']]]//*[@data-testid='editable-name']`));
    }

    async getScraperCardNameEditField(name){
        return await this.driver.findElement(By.xpath(`//*[@data-testid='input-field'][@value='${name}']`));
    }

    async getScraperCardDeleteByName(name){
        return await this.driver.findElement(By.xpath(`//*[@data-testid='resource-card'][.//span[text()='${name}']]//*[@data-testid='context-menu']`));
    }

    async getScraperCardDeleteConfirmByName(name){
        return await this.driver.findElement(By.xpath(`//*[@data-testid='resource-card'][.//span[text()='${name}']]//*[@data-testid='confirmation-button']`));
    }

    async getScrapersFilter(){
        return this.driver.findElement(By.css(scrapersFilter));
    }

    async getCreateScraperHeader(){
        return this.driver.findElement(By.css(createScraperHeader));
    }

    async getNameSort(){
        return this.driver.findElement(By.css(nameSort));
    }

    async getUrlSort(){
        return this.driver.findElement(By.css(urlSort));
    }

    async getBucketSort(){
        return this.driver.findElement(By.css(bucketSort));
    }

    async getCreateScraperEmpty(){
        return this.driver.findElement(By.css(createScraperEmpty));
    }

    static getCreateScraperEmptySelector(){
        return { type: 'css', selector: createScraperEmpty};
    }

    async getScraperCards(){
        return await this.driver.findElements(By.css(scraperCards));
    }


}

module.exports = scrapersTab;
