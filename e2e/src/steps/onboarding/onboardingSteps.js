const expect = require('chai').expect;
const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const splashPage = require(__srcdir + '/pages/onboarding/splashPage.js');
const initialSetupPage = require(__srcdir + '/pages/onboarding/initialSetupPage.js');
const readyPage = require(__srcdir + '/pages/onboarding/readyPage.js');
const influxPage = require(__srcdir + '/pages/influxPage.js');
const bucketsTab = require(__srcdir + '/pages/loadData/bucketsTab.js');


class onboardingSteps extends baseSteps {

    constructor(driver){
        super(driver);
        this.splashPage = new splashPage(driver);
        this.initialSetupPage = new initialSetupPage(driver);
        this.readyPage = new readyPage(driver);
        this.influxPage = new influxPage(driver);
        this.bucketsTab = new bucketsTab(driver);
    }

    async open(){
        this.spashPage.open();
    }

    async verifyHeadContains(text){

        await this.splashPage.getHeadMain().then(elem => {
            elem.getText().then(eltxt => {
                expect(eltxt)
                    .to
                    .include(text);
            });
        });
    }

    async verifyCreditsLink(){
        await this.splashPage.getCreditsLink().then( elem => {
            elem.getText().then( eltxt => {
                expect(eltxt).to.equal('InfluxData');
            });

            elem.getAttribute('href').then(href => {
                expect(href).to.equal('https://www.influxdata.com/');
            });
        });
    }

    async clickStart(){
        await this.splashPage.getStartButton().then( async elem => {
            await elem.click();
            await this.splashPage.waitUntilElementCss('h3.wizard-step--title ');
        });
    }

    async verifySetupHeaderContains(text){
        await this.initialSetupPage.getHeaderMain().then(elem => {
            elem.getText().then( async eltxt => {
                await expect(eltxt).to.include(text);
            });
        });
    }

    async verifyNavCrumbText(crumb, text){
        await this.initialSetupPage.getCrumbStep(crumb).then( async elem => {
            await elem.getText().then(async eltxt => {
                await expect(eltxt).to.include(text);
            });
        });

    }

    async setInputFieldValue(field, value){
        await this.initialSetupPage.getInputField(field).then( async elem => {
            await this.clearInputText(elem);
            //await elem.clear();
            await elem.sendKeys(value);
            //   await this.delay(3000)
        });
    }

    async clickContinueButton(chkReadyPage = true){
        await this.initialSetupPage.getNextButton().then(async btn => {
            await btn.click();
            if(chkReadyPage) {
                await this.readyPage.isLoaded();
            }
        }).catch(async err => {
            console.log(err);
        });
    }

    async verifySubtitle(){
        await this.readyPage.getSubtitle().then( async subt => {
            await subt.getText().then( async subtxt => {
                await expect(subtxt).to.include('1 organization');
                await expect(subtxt).to.include('1 user');
                await expect(subtxt).to.include('1 bucket');
            });
        });
    }

    async verifyNavCrumbTextColor(crumb, color){
        await this.initialSetupPage.getCrumbStep(crumb).then( async elem => {
            await elem.getCssValue('color').then(async cssColor => {
                await expect(cssColor).to.include(color);
            });
        });
    }

    async clickQuickStartButton(){
        await this.readyPage.getQuickStartButton().then(async btn =>{
            await btn.click();
            await this.influxPage.isLoaded();
            //await this.driver.sleep(1000) //for some reason if no wait here next page load throws error
        });
    }

    async clickAdvancedButton(){
        await this.readyPage.getAdvancedButton().then(async btn =>{
            await btn.click();
            await this.influxPage.isLoaded();
        });
    }

    async verifyFormErrorMessage(message){
        await this.initialSetupPage.getFormErrorMessage().then(async elem => {
            await elem.getText().then( eltxt => {
                expect(eltxt).to.equal(message);
            });
        });
    }

    async verifyFormErrorMessageNotPresent(){
        expect(await this.initialSetupPage.isFormErrorDisplayed()).to.be.false;
    }

    async verifyContinueButtonDisabled(){
        expect(await this.initialSetupPage.isNextButtonEnabled()).to.be.false;
    }

    async failTest(){
        await expect(true).to.be.false;
    }

}

module.exports = onboardingSteps;
