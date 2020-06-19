const expect = require('chai').expect;
const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const signinPage = require(__srcdir + '/pages/signin/signinPage.js');
const influxPage = require(__srcdir + '/pages/influxPage.js');

class signinSteps extends baseSteps {

    constructor(driver){
        super(driver);
        this.signinPage = new signinPage(driver);
        this.influxPage = new influxPage(driver);
    }

    async verifyHeadingContains(text){
        expect(await (await this.signinPage.getHeading()).getText()).to.include(text);
    }

    async verifyHeading(){
        await this.assertVisible(await this.signinPage.getInfluxLogo());
    }

    async verifyVersionContains(version){
        expect(await (await this.signinPage.getVersionInfo()).getText()).to.include(version);
    }

    async verifyCreditsLink(){
        await this.signinPage.getCreditsLink().then(  elem => {
            elem.getText().then( eltxt => {
                expect(eltxt).to.equal('InfluxData');
            });

            elem.getAttribute('href').then(href => {
                expect(href).to.equal('https://www.influxdata.com/');
            });
        });
    }

    async verifyIsLoaded(){
        this.assertVisible(await this.signinPage.getInfluxLogo());
        this.assertVisible(await this.signinPage.getNameInput());
        this.assertVisible(await this.signinPage.getPasswordInput());
        this.assertVisible(await this.signinPage.getSigninButton());
        //this.assertVisible(await this.signinPage.getCreditsLink());
    }

    async enterUsername(name){
        await this.signinPage.getNameInput().then(async input => {
            await input.clear();
            await input.sendKeys(name);
        });
    }

    async enterPassword(password){
        await this.signinPage.getPasswordInput().then(async input =>{
            await input.clear();
            await input.sendKeys(password);
        });
    }

    async clickSigninButton(){
        await this.signinPage.getSigninButton().then(async btn =>{
            await btn.click();
        });
    }

    async waitForSigninToLoad(timeout){
        await this.driver.wait(until.elementIsVisible(this.driver.findElement(By.css(selector))));
        await this.driver.wait(until.elementIsVisible(this.signinPage.getNameInput()), timeout,
            `Waited ${timeout} milliseconds to locate signin name input`);
        await this.driver.wait(until.elementIsVisible(this.signinPage.getPasswordInput()), timeout,
            `Waited ${timeout} milliseconds to locate signin pasword input`);

    }

    async signin(user){
        await this.enterUsername(user.username);
        await this.enterPassword(user.password);
        await this.clickSigninButton();
        await this.influxPage.isLoaded();
    }
}

module.exports = signinSteps;

