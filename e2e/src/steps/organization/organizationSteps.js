const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const organizationPage = require(__srcdir + '/pages/organization/organizationPage.js');


class organizationSteps extends influxSteps{

    constructor(driver){
        super(driver);
        this.orgPage = new organizationPage(__wdriver);
    }

    async isLoaded(){
        await this.orgPage.isLoaded();
    }

    async verifyIsLoaded(){
        // this.assertVisible(await this.setPage.getTabByName('Members')); no longer on page commit=bd91a81123 build_date=2020-04-07T07:57:22Z
        //        this.assertVisible(await this.setPage.getTabByName('Buckets')); moved to new load data page
        //        this.assertVisible(await this.setPage.getTabByName('Telegraf')); ditto
        //        this.assertVisible(await this.setPage.getTabByName('Scrapers')); ditto
        this.assertVisible(await this.orgPage.getTabByName('Members'));
        this.assertVisible(await this.orgPage.getTabByName('About'));
        // this.assertVisible(await this.setPage.getTabByName('Tokens')); // tokens no longer part of settings
        // this.assertVisible(await this.setPage.getTabByName('About')); no longer on page commit=bd91a81123 build_date=2020-04-07T07:57:22Z
    }

}

module.exports = organizationSteps;