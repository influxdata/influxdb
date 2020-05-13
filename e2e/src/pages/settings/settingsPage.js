const influxPage = require(__srcdir + '/pages/influxPage.js');
const { By } = require('selenium-webdriver');

const tabsCss = '[data-testid=tabs]';
const tabsXpath = '//*[@data-testid=\'tabs\']';

const urlCtx = 'settings';

class settingsPage extends influxPage {

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
        return await this.driver.findElement(By.xpath(`${tabsXpath}//div[@data-testid='tabs--tab' and @id='${name.toLowerCase()}']`));
    }

}

module.exports = settingsPage;
