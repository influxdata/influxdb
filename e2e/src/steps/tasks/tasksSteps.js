const influxSteps = require(__srcdir + '/steps/influx/influxSteps.js');
const tasksPage = require(__srcdir + '/pages/tasks/tasksPage.js');

class dashboardsSteps extends influxSteps {

    constructor(driver){
        super(driver);
        this.tasksPage = new tasksPage(driver);
    }

    async isLoaded(){
        await this.tasksPage.isLoaded();
    }

    async verifyIsLoaded(){
        await this.assertVisible(await this.tasksPage.getFilterTasks());
        await this.assertVisible(await this.tasksPage.getCreateTaskDropdownHeader());
        await this.assertVisible(await this.tasksPage.getInactiveToggle());
        await this.assertVisible(await this.tasksPage.getSortTypeButton());
        //this.assertVisible(await this.tasksPage.getNameSortButton());
        //this.assertVisible(await this.tasksPage.getActiveSortButton());
        //this.assertVisible(await this.tasksPage.getScheduleSortButton());
        //this.assertVisible(await this.tasksPage.getLastCompetedSortButton());
        await this.assertVisible(await this.tasksPage.getCreateTaskDropdownBody());
    }

    async verifyTaskCardVisible(name){
        await this.assertVisible(await this.tasksPage.getTaskCardByName(name));
    }

}

module.exports = dashboardsSteps;
