const basePage = require(__srcdir + '/pages/basePage.js');
const { By, until} = require('selenium-webdriver');

const subtitle = 'h5.wizard-step--sub-title:first-of-type';
const qStartButton = '[data-testid=button--quick-start]';
const advancedButton = '[data-testid=button--advanced]';
const laterButton = '[data-testid=button--conf-later]';
const urlCtx = '/onboarding/2';

class readyPage extends basePage {

    constructor(driver){
        super(driver);
    }

    async getSubtitle(){
        return this.driver.wait(until.elementLocated(By.css(subtitle)));
        //return await this.driver.findElement(By.css(subtitle))
    }

    async getQuickStartButton(){
        return await this.driver.findElement(By.css(qStartButton));
    }

    async getAdvancedButton(){
        return await this.driver.findElement(By.css(advancedButton));
    }


    async isLoaded(){
        await super.isLoaded([{type:'css', selector:subtitle},
            {type:'css', selector:qStartButton},
            {type:'css', selector:advancedButton},
            {type:'css', selector:laterButton}], urlCtx);
    }

}

module.exports = readyPage;
