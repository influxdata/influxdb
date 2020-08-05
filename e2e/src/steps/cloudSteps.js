const { until } = require('selenium-webdriver')

const baseSteps = require(__srcdir + '/steps/baseSteps.js');
//const createOrgPage = require(__srcdir + '/pages/createOrgPage.js');
const cloudLoginPage = require(__srcdir + '/pages/cloud/cloudLoginPage.js');
const influxUtils = require(__srcdir + '/utils/influxUtils.js');
const perfUtils = require(__srcdir + '/utils/performanceUtils.js');
const homePage = require(__srcdir + '/pages/home/homePage.js');

class cloudSteps extends baseSteps {

    constructor(driver){
        super(driver);
        //this.createOrgPage = new createOrgPage(driver);
        this.loginPage = new cloudLoginPage(driver);
        this.homePage = new homePage(driver);
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
            maxDelay, 'timely redirect failed');
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

    async logInToCloudTimed(maxDelay){
        await this.typeTextAndWait(await this.loginPage.getEmailInput(), __defaultUser.username);
        await this.typeTextAndWait(await this.loginPage.getPasswordInput(), __defaultUser.password);
        await perfUtils.execTimed(async () => {
            await this.clickAndWait(await this.loginPage.getLogInButton());
            try {
                await this.driver.wait(until.elementIsVisible(await this.homePage.getNavMenu()), maxDelay * 3);
                await this.driver.wait(until.elementIsVisible(await this.homePage.getPageHeader()), maxDelay * 3);
            }catch(err){
                console.warn(JSON.stringify(err));
                //try again
                await this.driver.wait(until.elementIsVisible(await this.homePage.getNavMenu()), maxDelay * 3);
                await this.driver.wait(until.elementIsVisible(await this.homePage.getPageHeader()), maxDelay * 3);
            }
        },maxDelay, 'failed to timely open cloud ');
    }

    //TODO - this is not checking correct page - see issue #19057
    //because currently not loading account info on logout like before
    //not sure why... once logout to correct page is fixed update this method
    //this is a holder so that other tests can be written
    async logoutToAccountInfoTimed(maxDelay){
        await perfUtils.execTimed(async () => {
           await this.clickAndWait(await this.homePage.getLogoutButton());
           await this.driver.wait(this.homePage.isLoaded(), maxDelay * 3);
        }, maxDelay, 'failed to timely open info');
    }

    async logoutToLoginTimed(maxDelay){
        await perfUtils.execTimed(async () => {
            await this.clickAndWait(await this.homePage.getLogoutButton());
            await this.loginPage.waitToLoad(10000);
        }, maxDelay, 'login slow to reload');
    }

    async verifyCloudLoginPageLoaded(){
        await this.loginPage.isLoaded();
    }



}

module.exports = cloudSteps;
