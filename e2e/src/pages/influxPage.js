const basePage = require(__srcdir + '/pages/basePage.js');
const { By } = require('selenium-webdriver');

const navMenu = '[data-testid=tree-nav]';
const navMenuHome = '[data-testid=tree-nav--header]';
const navMenuDExplorer = '[data-testid=nav-item-data-explorer]';
const navMenuDashboards = '[data-testid=nav-item-dashboards]';
const navMenuTasks = '[data-testid=nav-item-tasks]';
const navMenuAlerting = '[data-testid=nav-item-alerting]'; //N.B. only available with alerting on
const navMenuLoadData = '[data-testid=nav-item-load-data]';
const navMenuSettings = '[data-testid=nav-item-settings]';
//const navMenuFeedback = '[data-testid=nav-menu--item] span.nav-chat';
const navMenuUser = '[data-testid=user-nav]';
const navMenuOrg = '[data-testid=nav-item-org]';

const navMenuHomeHeading = '//a[text() = \'admin (qa)\']';
const navMenuHomeNewOrg = 'a.cf-nav--sub-item[href=\'/orgs/new\']';
const navMenuHomeLogout = 'a.cf-nav--sub-item[href=\'/logout\']';

const navMenuXpath = '//*[@data-testid = \'nav-menu\']';
const userMenuItemXpath = '//*[@data-testid = \'user-nav\']';
const userMenuItem = '[data-testid^=\'user-nav-item-%ITEM%\']';

const pageHeader = '[data-testid=page-header]';

const urlCtx = 'orgs';

class influxPage extends basePage {

    constructor(driver){
        super(driver);
    }

    async getNavMenu(){
        return await this.driver.findElement(By.css(navMenu));
    }

    async getPageHeader(){
        return await this.driver.findElement(By.css(pageHeader));
    }


    /*
    async isLoaded(){
        await super.isLoaded([{type:'css', selector:navMenu}], urlCtx)
    }*/

    //N.B. Method overloading not supported in JS - however this page is extended

    async isLoaded(selectors = undefined, url = undefined){
        if(!selectors){
            await super.isLoaded([{type:'css', selector:navMenu},
                {type: 'css', selector: navMenuHome},
                {type: 'css', selector: navMenuDExplorer},
                {type: 'css', selector: navMenuDashboards},
                {type: 'css', selector: navMenuTasks},
                //{type: 'css', selector: navMenuAlerting}, //N.B. only available with alerting on
                {type: 'css', selector: navMenuLoadData},
                {type: 'css', selector: navMenuSettings},
                {type: 'css', selector: navMenuUser},
            //    {type: 'css', selector: navMenuOrg}, // TODO - reactivate when certain orgs should be in menu
                {type: 'css', selector: pageHeader}
            ], urlCtx);
            return;
        }

        if(url){
            await super.isLoaded(selectors.concat([{type: 'css', selector: navMenu},
                {type: 'css', selector: pageHeader}]),url);
        }else{
            await super.isLoaded(selectors.concat([{type: 'css', selector: navMenu},
                {type: 'css', selector: pageHeader}]), urlCtx);
        }
    }

    async getMenuHome(){
        return await this.driver.findElement(By.css(navMenuHome));
    }

    async getMenuExplorer(){
        return await this.driver.findElement(By.css(navMenuDExplorer));
    }

    async getMenuDashboards(){
        return await this.driver.findElement(By.css(navMenuDashboards));
    }

    async getMenuTasks(){
        return await this.driver.findElement(By.css(navMenuTasks));
    }

    /* N.B. only available with alerting enabled
    async getMenuAlerting(){
        return await this.driver.findElement(By.css(navMenuAlerting));
    }
    */

    async getMenuLoadData(){
        return await this.driver.findElement(By.css(navMenuLoadData));
    }

    async getMenuSettings(){
        return await this.driver.findElement(By.css(navMenuSettings));
    }

    async getMenuFeedback(){
        return await this.driver.findElement(By.css(navMenuFeedback));
    }

    async getMenuHomeHeading(){
        return await this.driver.findElement(By.xpath(navMenuHomeHeading));
    }

    async getMenuHomeNewOrg(){
        return await this.driver.findElement(By.css(navMenuHomeNewOrg));
    }

    async getMenuHomeLogout(){
        return await this.driver.findElement(By.css(navMenuHomeLogout));
    }

    async getNavMenuAlerting(){
        return await this.driver.findElement(By.css(navMenuAlerting));
    }

    async getSubItemByText(text){
        return await this.driver.findElement(By.xpath(navMenuXpath + `//*[text() = '${text}']`));
    }

    async getSubItemContainingText(text){
        return await this.driver.findElement(By.xpath(navMenuXpath + `//*[contains(text(), '${text}')]`));
    }

    async getNavMenuUser(){
        return await this.driver.findElement(By.css(navMenuUser));
    }

    async getNavMenuOrg(){
        return await this.driver.findElement(By.css(navMenuOrg));
    }

    async getUserMenuSwitchOrg(){
        return await this.driver.findElement(By.xpath( `${userMenuItemXpath}[text() = 'Switch Organizations']`));
    }

    async getUserMenuCreateOrg(){
        return await this.driver.findElement(By.xpath(`${userMenuItemXpath}[text() = 'Create Organization']`));
    }

    async getUserMenuLogout(){
        return await this.driver.findElement(By.xpath(`${userMenuItemXpath}[text() = 'Logout']`));
    }

    async getUserMenuItem(item){
        return await this.driver.findElement(By.css(userMenuItem.replace('%ITEM%', item.toLowerCase())));
    }


}

module.exports = influxPage;
