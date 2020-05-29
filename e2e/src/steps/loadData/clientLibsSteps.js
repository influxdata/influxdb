const { By } = require('selenium-webdriver');
const { expect } = require('chai');
var ncp = require('copy-paste');

const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const clientLibsTab = require(__srcdir + '/pages/loadData/clientLibsTab.js');

class clientLibsSteps extends baseSteps {

    constructor(driver) {
        super(driver);
        this.clibTab = new clientLibsTab(driver);
    }

    async isLoaded() {
        await this.clibTab.isTabLoaded();
    }

    async verifyClientLibsTabIsLoaded(){
        await this.clibTab.isTabLoaded();
        await this.assertVisible(await this.clibTab.getClientURL());
        await this.assertVisible(await this.clibTab.getLibTileByName('csharp'));
        await this.assertVisible(await this.clibTab.getLibTileByName('java'));
        await this.assertVisible(await this.clibTab.getLibTileByName('python'));
    }

    async clickLibTileByName(name){
        await this.clickAndWait(await this.clibTab.getLibTileByName(name));
    }

    async verifyCSharpPopupLoaded(){
        //await this.clickAndWait(await this.clibTab.getCopy2ClipByLabel('Package Manager'));
        await this.assertVisible(await this.clibTab.getPopupDismiss());
        await this.assertVisible(await this.clibTab.getPopupBody());
        await this.verifyElementContainsText(await this.clibTab.getPopupTitle(), 'C# Client Library');
    }

    async verifyGoPopupLoaded(){
        await this.assertVisible(await this.clibTab.getPopupDismiss());
        await this.assertVisible(await this.clibTab.getPopupBody());
        await this.verifyElementContainsText(await this.clibTab.getPopupTitle(), 'GO Client Library');
    }

    async verifyJavaPopupLoaded(){
        await this.assertVisible(await this.clibTab.getPopupDismiss());
        await this.assertVisible(await this.clibTab.getPopupBody());
        await this.verifyElementContainsText(await this.clibTab.getPopupTitle(), 'Java Client Library');
    }

    async verifyNodePopupLoaded(){
        await this.assertVisible(await this.clibTab.getPopupDismiss());
        await this.assertVisible(await this.clibTab.getPopupBody());
        await this.verifyElementContainsText(await this.clibTab.getPopupTitle(), 'JavaScript/Node.js Client Library');
    }

    async verifyPythonPopupLoaded(){
        await this.assertVisible(await this.clibTab.getPopupDismiss());
        await this.assertVisible(await this.clibTab.getPopupBody());
        await this.verifyElementContainsText(await this.clibTab.getPopupTitle(), 'Python Client Library');
    }

    async clickCopyToClipboardText(label){
        await this.clickAndWait(await this.driver.findElement(By.xpath(`//p[text() = '${label}']/following-sibling::div[1]//button`)));//can be slow to perform copy
    }

    async verifyClipboardTextFrom(label){
        let text = await  (await this.driver.findElement(By.xpath(`//p[text() = '${label}']/following-sibling::div[1]//code`))).getText();
        console.log('DEBUG text ' + text);
        let clipboard = ncp.paste();
        console.log ('DEBUG clipboard ' + clipboard);
    }

    async verifyPopupGithubLink(token){

        await this.clibTab.getPopupGithubLink().then(async link => {
            await link.getAttribute('href').then( async href => {
                expect(href).to.include(token);
            });
        });

    }
}

module.exports = clientLibsSteps;
