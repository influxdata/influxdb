const basePage = require(__srcdir + '/pages/basePage.js');
const { By } = require('selenium-webdriver');

const headMain = '[data-testid=init-step--head-main]';
const creditsLink = '[data-testid=credits] a';
const startButton =  '[data-testid=onboarding-get-started]';

class splashPage extends basePage {

    constructor(driver){
        super(driver);
    }

    async getHeadMain(){
        //promises 201 - passing promisses between methods
        //N.B. returns a promise wrapping the element
        return await this.driver.findElement(By.css(headMain));
    }

    async getCreditsLink(){
        return await this.driver.findElement(By.css(creditsLink));
    }

    async getStartButton(){
        return await this.driver.findElement(By.css(startButton));
    }

}

module.exports = splashPage;
