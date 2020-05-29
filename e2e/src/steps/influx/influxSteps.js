//const assert = require('chai').assert;
const expect = require('chai').expect;
const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const influxPage = require(__srcdir + '/pages/influxPage.js');

class influxSteps extends baseSteps {

    constructor(driver){
        super(driver);
        this.influxPage = new influxPage(driver);
    }

    async isLoaded(){
        await this.influxPage.isLoaded();
    }

    async verifyIsLoaded(){
        this.assertVisible(await this.influxPage.getNavMenu());
        this.assertVisible(await this.influxPage.getMenuHome());
        this.assertVisible(await this.influxPage.getMenuExplorer());
        this.assertVisible(await this.influxPage.getMenuDashboards());
        this.assertVisible(await this.influxPage.getMenuTasks());
        this.assertVisible(await this.influxPage.getMenuSettings());
        //this.assertVisible(await this.influxPage.getMenuFeedback());
    }

    async getNavMenuElem(item){
        let elem = undefined;
        switch(item.toLowerCase()){
        case 'home':
            elem = await this.influxPage.getMenuHome();
            break;
        case 'home:heading':
            elem = await this.influxPage.getMenuHomeHeading();
            break;
        case 'home:neworg':
            elem = await this.influxPage.getMenuHomeNewOrg();
            break;
        case 'home:logout':
            elem = await this.influxPage.getMenuHomeLogout();
            break;
        case 'explorer':
            elem = await this.influxPage.getMenuExplorer();
            break;
        case 'dashboards':
            elem = await this.influxPage.getMenuDashboards();
            break;
        case 'tasks':
            elem = await this.influxPage.getMenuTasks();
            break;
        case 'settings':
            elem = await this.influxPage.getMenuSettings();
            break;
       // case 'feedback':
       //     elem = await this.influxPage.getMenuFeedback();
       //     break;
        case 'alerting':
            elem = await this.influxPage.getNavMenuAlerting();
            break;
        case 'loaddata':
            elem = await this.influxPage.getMenuLoadData();
            //            await this.driver.executeScript('arguments[0].style.border=\'3px solid red\'', elem);
            break;
        case 'organization':
            elem = await this.influxPage.getNavMenuOrg();
            break;
        case 'user':
            elem = await this.influxPage.getNavMenuUser();
            break;
        default:
            throw `Unkown menu item ${item}`;
        }

        return elem;
    }

    async getUserMenuElem(item){
        let elem = undefined;
        switch(item){
            case 'switchOrg':
                elem = await this.influxPage.getUserMenuItem('switch-orgs');
                break;
            case 'createOrg':
                elem = await this.influxPage.getUserMenuItem('create-orgs');
                break;
            case 'logout':
                elem = await this.influxPage.getUserMenuItem('logout');
                break;
            default:
                throw `Unkown menu item ${item}`;
        }

        return elem;
    }

    async hoverNavMenu(item){
        await this.hoverOver(await this.getNavMenuElem(item));
    }

    async verifySubMenuItems(item, state = 'hidden'){
        if(state === 'hidden'){
            this.assertNotVisible(await this.getNavMenuElem(item));
        }else if(state === 'visible'){
            this.assertVisible(await this.getNavMenuElem(item));
        }else{
            throw `unkown menu state ${state}`;
        }
    }

    async verifyUserMenuItems(item, state = 'hidden'){
        if(state === 'hidden'){
            this.assertNotVisible(await this.getUserMenuElem(item));
        }else if(state === 'visible'){
            this.assertVisible(await this.getUserMenuElem(item));
        }else{
            throw `unkown menu state ${state}`;
        }
    }

    async clickMenuItem(item,
                        wait = async () => { await this.driver.sleep((await this.driver.manage().getTimeouts()).implicit/20); }){
        await this.clickAndWait(await this.getNavMenuElem(item), wait);
    }

    async clickSubMenuItem(item,
                           wait = async () => { await this.driver.sleep(1000); }){
        if(item.toLowerCase() === 'dashboards'){//troubleshoot issue in circleci
            await this.driver.sleep(1000); //wait a second for page to load
            await this.clickAndWait(await this.influxPage.getSubItemByText(item), async () => {
                //N.B. sometimes page will have no cells - so waiting for cells won't always work
                //However CircleCI issue is that cells are not showing up
                await this.driver.sleep(3000);
            });

        }else {
            await this.clickAndWait(await this.influxPage.getSubItemByText(item), wait);
        }
    }

    async verifyVisibilityNavItemByText(text, visible = true){
        if(visible) {
            await this.assertVisible(await this.influxPage.getSubItemByText(text));
        }else{
            await this.assertNotVisible(await this.influxPage.getSubItemByText(text));
        }
    }

    async verifyHeaderContains(text){
        await this.influxPage.getPageHeader().then(async elem => {
            await elem.getText().then(async elTxt => {
                expect(elTxt).to.include(text);
            });
        });
    }

    async verifyFeedbackLinkContains(text){
        await this.getNavMenuElem('feedback').then(async elem => {
            await elem.findElement({xpath: './..'}).then(async el2 => { //the getNavMenuElem method uses span inside a tag
                await el2.getAttribute('href').then(async attrib =>{
                    expect(attrib).to.include(text);
                });
            });
        });
    }


}

module.exports = influxSteps;
