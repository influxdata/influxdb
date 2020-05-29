const basePage = require(__srcdir + '/pages/basePage.js');
const { By, until} = require('selenium-webdriver');

const headerMain = '[data-testid=admin-step--head-main]';
const navCrumbToken = 'data-testid=nav-step--';
const inputFieldToken = 'data-testid=input-field--';
const nextButton = '[data-testid=next]';
const formErrorMessage = '[data-testid=form--element-error]';


class initialSetupPage extends basePage {

    constructor(driver){
        super(driver);
    }

    async getHeaderMain(){
        return await this.driver.findElement(By.css(headerMain));
    }


    async getCrumbStep(step){
        return await this.driver.findElement(By.css(`[${navCrumbToken}${step}]`));
    }

    async getInputField(name){
        return await this.driver.findElement(By.css(`[${inputFieldToken}${name}]`));
    }

    async getNextButton(){
        return this.driver.wait(until.elementLocated(By.css(nextButton)));
        //return await this.driver.findElement(By.css(nextButton))
    }

    async getFormErrorMessage(){
        return this.driver.wait(until.elementLocated(By.css(formErrorMessage)));
    }

    async isFormErrorDisplayed(){
        try {
            await this.driver.findElement(By.css(formErrorMessage)).isDisplayed();
            return true;
        }catch(err){
            if(err.name === 'NoSuchElementError'){
                return false;
            }else{
                throw err;
            }
        }
    }

    async isNextButtonEnabled(){
        return await this.driver.findElement(By.css(nextButton)).isEnabled();
    }

}

module.exports = initialSetupPage;
