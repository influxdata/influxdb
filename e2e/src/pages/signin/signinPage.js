const basePage = require(__srcdir + '/pages/basePage.js');
const { By } = require('selenium-webdriver');

const heading = '.splash-page--heading';
const influxLogo = '[data-testid=logo--influxdb-cloud]';
const versionInfo = '.version-info p';
const creditsLink = '.splash-page--credits a';
const nameInput = 'input[name=username]'; //TODO - see if data-testid can be updated - currently 'input-field' for both name and password inputds
const passwordInput = 'input[name=password]'; //TODO - see if data-testid can be updated
const signinButton = '[data-testid=button]';

const urlCtx = 'signin';

class signinPage extends basePage{

    constructor(driver){
        super(driver);
        this.urlCtx = urlCtx;
    }

    async getHeading(){
        return await this.driver.findElement(By.css(heading));
    }

    async getVersionInfo(){
        return await this.driver.findElement(By.css(versionInfo));
    }

    async getCreditsLink(){
        return await this.driver.findElement(By.css(creditsLink));
    }

    async getNameInput(){
        return await this.driver.findElement(By.css(nameInput));
    }

    async getPasswordInput(){
        return await this.driver.findElement(By.css(passwordInput));
    }

    async getSigninButton(){
        return await this.driver.findElement(By.css(signinButton));
    }

    async getInfluxLogo(){
        return await this.driver.findElement(By.css(influxLogo));
    }

}

module.exports = signinPage;
