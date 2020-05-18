const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const homePage = require(__srcdir + '/pages/home/homePage.js');
const { By } = require('selenium-webdriver');

class homeSteps extends influxSteps {

    constructor(driver){
        super(driver);
        this.homePage = new homePage(driver);
    }

    async isLoaded(){
        await this.homePage.isLoaded();
    }

    async verifyIsLoaded(){
        this.assertVisible(await this.homePage.getLogoutButton());
        this.assertVisible(await this.homePage.getGetStartedDataCollect());
        this.assertVisible(await this.homePage.getGetStartedDashboard());
        this.assertVisible(await this.homePage.getGetStartedAlerting());
        this.assertVisible(await this.homePage.getTutorialsList());
        this.assertVisible(await this.homePage.getUsefulLinksList());
        this.assertVisible(await this.homePage.getTutorialLinkByText('Get Started with Flux'));
        this.assertVisible(await this.homePage.getUsefulLinkByText('Documentation'));
    }

    async clickLogout(){
        await this.homePage.getLogoutButton().then(async btn => {
            await btn.click();
        });
    }

    async clickQuickStartPanel(title){
        switch(title.toLowerCase()){
        case 'data collector':
            await (await this.homePage.getDataCollectButton()).click();
            break;
        case 'dashboard':
            await (await this.homePage.getDashboardButton()).click();
            break;
        case 'alerting':
            await (await this.homePage.getAlertingButton()).click();
            break;
        default:
            throw `Unknown Quick Start Panel: ${title}`;
        }
    }

    async verifyDbdPanelDashboard(dbdName){
        this.homePage.getDashboardsList().then(async elem => {
            await elem.findElement(By.xpath(`.//a[text() = '${dbdName}']`)).then(async link => {
                await this.assertVisible(link);
            });
        });
    }

    async clickDbdPanelDashboard(dbdName){
        await this.homePage.getDashboardsList().then(async elem => {
            await elem.findElement(By.xpath(`.//a[text() = '${dbdName}']`)).then(async link => {
                await link.click();
            });
        });
    }

    async verifyDashboardLinksInPanel(links){
        let linksArr = links.split(',');
        for(let i = 0; i < linksArr.length; i++){
            await this.assertVisible(await this.homePage.getDashboardLink(linksArr[i].trim()));
        }
    }

    async clickDashboardLinkFromPanel(link){
        await this.clickAndWait(await this.homePage.getDashboardLink(link));
    }

}


module.exports = homeSteps;
