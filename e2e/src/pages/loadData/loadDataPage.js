const influxPage = require(__srcdir + '/pages/influxPage.js');
const { By } = require('selenium-webdriver');

const tabsCss = '[data-testid=tabs]';
const tabsXpath = '//*[@data-testid=\'tabs\']';
const pageTitle = '[data-testid=\'page-title\']';

const urlCtx = 'settings';

//Create Scraper Popup - accessible in both Scraper Page and base LoadData page - through buckets add data
const createScraperTitle = '[data-testid=overlay--header] [class*=title]';
const createScraperDismiss = '[data-testid=overlay--header] button';
const createScraperNameInput = '[data-testid=input-field][title=Name]';
const createScraperBucketDropdown = '[data-testid=bucket-dropdown--button]';
const createScraperUrlInput = '[data-testid=input-field][title*=URL]';
const createScraperCancel = '[data-testid=create-scraper--cancel]';
const createScraperSubmit = '[data-testid=create-scraper--submit]';
const createScraperBucketDropdownItems = '[data-testid=bucket-dropdown] [data-testid=dropdown-item]';

//Create Telegraf Config Wizard - accessible from buckets and telegraf
const bucketDropdown = '[data-testid=bucket-dropdown]';
const pluginsFilter = '[data-testid=input-field][placeholder*=Plugins]';
const telegrafNameInput = '[data-testid=input-field][title$=\'Name\']';
const telegrafDescrInput = '[data-testid=input-field][title$=\'Description\']';
//common controls in basePage
const codeSnippetWithToken = 'div.code-snippet:first-of-type';
const codeSnippetWithTelegrafCommand = 'div.code-snippet:nth-of-type(2)';


class loadDataPage extends influxPage {

    constructor(driver){
        super(driver);
    }

    async isLoaded(){
        await super.isLoaded([{type: 'css', selector: tabsCss}], urlCtx);
    }

    async isTabLoaded(tabUrlPart, selectors = undefined){
        if(selectors) {
            await super.isLoaded(selectors.concat([{type: 'css', selector: tabsCss}]), tabUrlPart);
        }else{
            await super.isLoaded([{type: 'css', selector: tabsCss}], tabUrlPart);
        }
    }

    async getTabByName(name){
        return await this.driver.findElement(By.xpath(`${tabsXpath}//div[@data-testid='tabs--tab' and @id='${name.toLowerCase().replace(' ', '-')}']`));
    }

    async getPageTitle(){
        return await this.driver.findElement(By.css(pageTitle));
    }

    //Create Scraper Popup
    async getCreateScraperTitle(){
        return await this.driver.findElement(By.css(createScraperTitle));
    }

    static getCreateScraperTitleSelector(){
        return { type: 'css', selector: createScraperTitle};
    }

    async getCreateScraperDismiss(){
        return await this.driver.findElement(By.css(createScraperDismiss));
    }

    async getCreateScraperNameInput(){
        return await this.driver.findElement(By.css(createScraperNameInput));
    }

    static getCreateScraperNameInputSelector(){
        return { type: 'css', selector: createScraperNameInput};
    }

    async getCreateScraperBucketDropdown(){
        return await this.driver.findElement(By.css(createScraperBucketDropdown));
    }

    async getCreateScraperUrlInput(){
        return await this.driver.findElement(By.css(createScraperUrlInput));
    }

    static getCreateScraperUrlInputSelector(){
        return { type: 'css', selector: createScraperUrlInput};
    }

    async getCreateScraperCancel(){
        return await this.driver.findElement(By.css(createScraperCancel));
    }

    async getCreateScraperSubmit(){
        return await this.driver.findElement(By.css(createScraperSubmit));
    }

    async getCreateScraperBucketDropdownItem(item){
        return await this.driver.findElement(By.xpath(`//*[@data-testid='dropdown-item']/div[text() = '${item}']`));
    }

    static getCreateScraperBucketDropdownItemSelector(item){
        return `//*[@data-testid='dropdown-item']/div[text() = '${item}']`;
    }

    static getCreateScraperBucketDropdownItemsSelector(){
        return {type: 'css', selector: createScraperBucketDropdownItems};
    }

    //Create telegraf wizard
    async getBucketDropdown(){
        return await this.driver.findElement(By.css(bucketDropdown));
    }

    async getPluginsFilter(){
        return await this.driver.findElement(By.css(pluginsFilter));
    }

    async getPluginTileByName(name){
        return await this.driver.findElement(By.css(`[data-testid=telegraf-plugins--${name}]`));
    }

    async getBucketDropdownItem(item){
        return await this.driver.findElement(
            By.xpath(`//*[@data-testid='dropdown-item'][div[text()='${item}']]`));
    }

    async getTelegrafNameInput(){
        return await this.driver.findElement(By.css(telegrafNameInput));
    }

    async getTelegrafDescrInput(){
        return await this.driver.findElement(By.css(telegrafDescrInput));
    }

    async getCodeSnippetWithToken(){
        return await this.driver.findElement(By.css(codeSnippetWithToken));
    }

    async getCodeSnippetWithTelegrafCommand(){
        return await this.driver.findElement(By.css(codeSnippetWithTelegrafCommand));
    }


}

module.exports = loadDataPage;
