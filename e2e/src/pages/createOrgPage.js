const basePage = require(__srcdir + '/pages/basePage.js');
const { By } = require('selenium-webdriver');

const formOverlay = '[data-testid=overlay--children]';
const formOverlayHeader =  '[data-testid=overlay--header]';
const formOverlayDismiss = '[data-testid=overlay--header] button.cf-overlay--dismiss';
const inputOrgName = '[placeholder*=organization]';
const inputBucketName = '[placeholder*=bucket]';
const buttonCancel = 'button[title=Cancel]';
const buttonCreate = 'button[title=Create]';

const urlCtx = 'orgs/new';

class createOrgPage extends basePage {

    constructor(driver){
        super(driver);
    }

    async isLoaded(){
        await super.isLoaded([{type: 'css', selector: formOverlay},
            {type: 'css', selector: formOverlayHeader},
            {type: 'css', selector: formOverlayDismiss},
            {type: 'css', selector: inputOrgName},
            {type: 'css', selector: inputBucketName},
            {type: 'css', selector: buttonCancel},
            {type: 'css', selector: buttonCreate},
        ], urlCtx);
    }

    async getInputOrgName(){
        return await this.driver.findElement(By.css(inputOrgName));
    }

    async getInputBucketName(){
        return await this.driver.findElement(By.css(inputBucketName));
    }

    async getbuttonCancel(){
        return await this.driver.findElement(By.css(buttonCancel));
    }

    async getbuttonCreate(){
        return await this.driver.findElement(By.css(buttonCreate));
    }



}

module.exports = createOrgPage;
