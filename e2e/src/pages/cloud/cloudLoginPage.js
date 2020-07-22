const { By, Condition, until, StaleElementReferenceError} = require('selenium-webdriver');

const urlCtx = 'login';

const emailInput = '[data-testid=\'input-field\']';
const passwordInput = '[data-testid=\'visibility-input\']';
const logInButton = '[data-testid=\'button\']';
const logInPanel = '[data-testid=\'panel\']';

class cloudLoginPage {

    constructor(driver) {
        //super(driver);
        this.urlCtx = urlCtx;
        this.driver = driver;
    }

    async waitToLoad(timeout = 10000){
        await this.driver.wait(until.elementLocated(By.css(logInPanel)), timeout,
            `Login controls failed to load in ${timeout} milliseonds `);
    }

    async getEmailInput(){
        return await this.driver.findElement(By.css(emailInput));
    }

    async getPasswordInput(){
        return await this.driver.findElement(By.css(passwordInput));
    }

    async getLogInButton(){
        return await this.driver.findElement(By.css(logInButton));
    }

}

module.exports = cloudLoginPage;

