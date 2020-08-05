const influxPage = require(__srcdir + '/pages/influxPage.js');
const { By } = require('selenium-webdriver');

const logoutButton = '[title=Logout]';
const getStartedDataCollect = '//*[@data-testid=\'panel\'][./div[contains(@class, \'getting-started\')]][.//span[text()=\'Load your data\']]';
const getStartedDashboard = '//*[@data-testid=\'panel\'][./div[contains(@class, \'getting-started\')]][.//span[text()=\'Build a dashboard\']]';
const getStartedAlerting = '//*[@data-testid=\'panel\'][./div[contains(@class, \'getting-started\')]][.//span[text()=\'Set up alerting\']]';
const dataCollectButton = '[data-testid=button][title=\'Load your data\']';
const dashboardButton = '[data-testid=button][title=\'Build a dashboard\']';
const alertingButton = '[data-testid=button][title=\'Set up alerting\']';
const tutorialsList = '//ul[contains(@class, \'tutorials\')]';
const dashboardsList = '//*[./div/*[text()=\'Recent Dashboards\']]';
const usefulLinkList = '//div[contains(@class,\'cf-col-sm-4 cf-col-md-3\')]//div[@data-testid=\'panel\'][3]//ul';
//const dashboardLink = '//*[@data-testid=\'panel\'][.//*[text()=\'Dashboards\']]//ul/li/a[text()=\'%TEXT%\']';
const dashboardLink = '//*[@data-testid=\'panel\'][.//*[text()=\'Recent Dashboards\']]//*[text()=\'%TEXT%\']'

// TODO - add selectors - especially for isLoaded below

class homePage extends influxPage {

    constructor(driver){
        super(driver);
    }

    async isLoaded(){
        await super.isLoaded([//{type: 'css', selector: logoutButton},
            {type: 'xpath', selector: getStartedDataCollect},
            {type: 'xpath', selector: getStartedDashboard},
            {type: 'xpath', selector: getStartedAlerting},
            {type: 'xpath', selector: tutorialsList},
            {type: 'xpath', selector: usefulLinkList},
        ]);
    }

    async getLogoutButton(){
        return await this.driver.findElement(By.css(logoutButton));
    }

    async getGetStartedDataCollect(){
        return await this.driver.findElement(By.xpath(getStartedDataCollect));
    }

    async getGetStartedDashboard(){
        return await this.driver.findElement(By.xpath(getStartedDashboard));
    }

    async getGetStartedAlerting(){
        return await this.driver.findElement(By.xpath(getStartedAlerting));
    }

    async getTutorialsList(){
        return await this.driver.findElement(By.xpath(tutorialsList));
    }

    async getUsefulLinksList(){
        return await this.driver.findElement(By.xpath(usefulLinkList));
    }

    async getTutorialLinkByText(text){
        return await this.driver.findElement(By.xpath(`${tutorialsList}//a[text() = '${text}']`));
    }

    async getUsefulLinkByText(text){
        return await this.driver.findElement(By.xpath(`${usefulLinkList}//a[contains(text(), '${text}')]`));
    }

    async getDashboardsList(){
        return await this.driver.findElement(By.xpath(dashboardsList));
    }

    async getDataCollectButton(){
        return await this.driver.findElement(By.css(dataCollectButton));
    }

    async getDashboardButton(){
        return await this.driver.findElement(By.css(dashboardButton));
    }

    async getAlertingButton(){
        return await this.driver.findElement(By.css(alertingButton));
    }

    async getDashboardLink(text){
        return await this.driver.findElement(By.xpath(dashboardLink.replace('%TEXT%', text)));
    }
}

module.exports = homePage;
