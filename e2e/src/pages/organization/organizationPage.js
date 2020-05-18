const influxPage = require(__srcdir + '/pages/influxPage.js');
const { By } = require('selenium-webdriver');

const tabsCss = '[data-testid=tabs]';
const tabsXpath = '//*[@data-testid=\'tabs\']';

const urlCtx = 'members';

class organizationPage extends influxPage {

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
        return await this.driver.findElement(By.xpath(`${tabsXpath}//a[@data-testid='tabs--tab'][.//*[text()='${name}']]`));
    }

    //`//div[@class='site-bottom-wrapper'][descendant::a[contains(.,'${ siteName }')]]//div[contains(@class, 'manage-settings')]`

}

module.exports = organizationPage;