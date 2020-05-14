const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const dataExplorerPage = require(__srcdir + '/pages/dataExplorer/dataExplorerPage.js');

class dataExplorerSteps extends influxSteps{

    constructor(driver){
        super(driver);
        this.dePage = new dataExplorerPage(driver);
    }

    async isLoaded(){
        await this.dePage.isLoaded();
    }

    async verifyIsLoaded(){
        this.assertVisible(await this.dePage.getTimeLocaleDropdown());
        this.assertVisible(await this.dePage.getGraphTypeDropdown());
        this.assertVisible(await this.dePage.getCustomizeGraphButton());
        this.assertVisible(await this.dePage.getSaveAsButton());
        this.assertVisible(await this.dePage.getViewArea());
        this.assertVisible(await this.dePage.getViewRawToggle());
        this.assertVisible(await this.dePage.getAutoRefreshDropdown());
        this.assertVisible(await this.dePage.getTimeRangeDropdown());
        this.assertVisible(await this.dePage.getSubmitQueryButton());
    }



}

module.exports = dataExplorerSteps;
