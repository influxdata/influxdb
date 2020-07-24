const { By, Condition, until, StaleElementReferenceError} = require('selenium-webdriver');
const basePage = require(__srcdir + '/pages/basePage.js');

const emailInput = '[data-testid=\'input-field\']';
const passwordInput = '[data-testid=\'visibility-input\']';
const logInButton = '[data-testid=\'button\']';
const logInPanel = '[data-testid=\'panel\']';
const googleLoginButton = '[data-testid=button-base]';
const signupLink = '//*[text()=\'Sign Up\']';
const forgotPassLink = '//*[text()=\'Forgot Password\']';

const urlCtx = 'login';


class cloudLoginPage extends basePage {

    constructor(driver) {
        super(driver);
        this.urlCtx = urlCtx;
        this.driver = driver;
    }

    async isLoaded(){
        await super.isLoaded([
            {type: 'css', selector: emailInput},
            {type: 'css', selector: passwordInput},
            {type: 'css', selector: logInButton},
            {type: 'css', selector: logInPanel},
            {type: 'css', selector: googleLoginButton},
            {type: 'xpath', selector: signupLink},
            {type: 'xpath', selector: forgotPassLink}
        ], urlCtx);
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

