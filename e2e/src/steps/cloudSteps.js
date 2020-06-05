const baseSteps = require(__srcdir + '/steps/baseSteps.js');
//const createOrgPage = require(__srcdir + '/pages/createOrgPage.js');
const cloudLoginPage = require(__srcdir + '/pages/cloud/cloudLoginPage.js');
const influxUtils = require(__srcdir + '/utils/influxUtils.js');

class cloudSteps extends baseSteps {

    constructor(driver){
        super(driver);
        //this.createOrgPage = new createOrgPage(driver);
        this.loginPage = new cloudLoginPage(driver);
    }

    //for driver sync
    async isLoaded(){
        //await this.createOrgPage.isLoaded();
    }

    //for assrtions
    async verifyIsLoaded(){
        //this.assertVisible(await this.createOrgPage.getInputOrgName());
        //this.assertVisible(await this.createOrgPage.getInputBucketName());
        //this.assertVisible(await this.createOrgPage.getbuttonCancel());
        //this.assertVisible(await this.createOrgPage.getbuttonCreate());
    }

    async setupDefaultCloudUser(){
        await influxUtils.setupCloudUser('DEFAULT');
    }

    async openCloudLogin(){
        await this.openBase();
        //wait for login form to load
        await this.loginPage.waitToLoad(10000);

    }

    async logInToCloud(){
        await this.typeTextAndWait(await this.loginPage.getEmailInput(), __defaultUser.username);
        await this.typeTextAndWait(await this.loginPage.getPasswordInput(), __defaultUser.password);
        await this.clickAndWait(await this.loginPage.getLogInButton());
    }

    async checkJavaScriptClient(userName){
        console.log("--- Checking NodeJS Client ---");
        await influxUtils.checkNodeJSClient(userName);
    }

    async checkJavaScriptAPIClient(userName){
        console.log("--- Checking NodeJS Client API ---");
        await influxUtils.checkNodeJSClientAPI(userName);
    }

}

module.exports = cloudSteps;
