const baseSteps = require(__srcdir + '/steps/baseSteps.js');
//const createOrgPage = require(__srcdir + '/pages/createOrgPage.js');
const cloudLoginPage = require(__srcdir + '/pages/cloud/cloudLoginPage.js');
const influxUtils = require(__srcdir + '/utils/influxUtils.js');
const perfUtils = require(__srcdir + '/utils/performanceUtils.js');

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

    async openCloudPage(maxDelay){
        await perfUtils.execTimed(async () => {
                await this.driver.get(__config.influx_url);
                await this.loginPage.waitToLoad(10000);
            },
            maxDelay, `Redirect failed to resolve in ${maxDelay} ms`);
    }

    async performanceBogusTest(sleep, delay){
        await perfUtils.execTimed( async() => {
            await this.driver.sleep(sleep);
        }, delay, "bogus test failed", 'bogus test succeeded');
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

}

module.exports = cloudSteps;
