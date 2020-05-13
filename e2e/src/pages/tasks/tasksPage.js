const influxPage = require(__srcdir + '/pages/influxPage.js');
const basePage = require(__srcdir + '/pages/basePage.js');
const { By } = require('selenium-webdriver');

const filterTasks = '[data-testid=search-widget]';
const inactiveToggle = '[data-testid=slide-toggle]';
const createTaskDropdownHeader = '[data-testid=page-control-bar--right] [data-testid=add-resource-dropdown--button]';
const nameSortButton = '[data-testid=resource-list--sorter]:nth-of-type(1)';
const activeSortButton = '[data-testid=resource-list--sorter]:nth-of-type(2)';
const scheduleSortButton = '[data-testid=resource-list--sorter]:nth-of-type(3)';
const lastCompletedSortButton = '[data-testid=resource-list--sorter]:nth-of-type(4)';
// following is only present until first task is created, so not good candidate for isLoaded check
const createTaskDropdownBody = '[data-testid=resource-list--body] [data-testid=add-resource-dropdown--button]';

const urlCtx = 'tasks';

class tasksPage extends influxPage{

    constructor(driver){
        super(driver);
    }

    async isLoaded(){
        await super.isLoaded([{type: 'css', selector: filterTasks},
            {type: 'css', selector: inactiveToggle} ,
            {type: 'css', selector: createTaskDropdownHeader} ,
           // {type: 'css', selector: nameSortButton},
            basePage.getSortTypeButtonSelector(),
            //{type: 'css', selector: activeSortButton},
            //{type: 'css', selector: scheduleSortButton},
            //{type: 'css', selector: lastCompletedSortButton}
        ], urlCtx);
    }

    async getFilterTasks(){
        return await this.driver.findElement(By.css(filterTasks));
    }

    async getInactiveToggle(){
        return await this.driver.findElement(By.css(inactiveToggle));
    }

    async getCreateTaskDropdownHeader(){
        return await this.driver.findElement(By.css(createTaskDropdownHeader));
    }

    async getNameSortButton(){
        return await this.driver.findElement(By.css(nameSortButton));
    }

    async getActiveSortButton(){
        return await this.driver.findElement(By.css(activeSortButton));
    }

    async getScheduleSortButton(){
        return await this.driver.findElement(By.css(scheduleSortButton));
    }

    async getLastCompetedSortButton(){
        return await this.driver.findElement(By.css(lastCompletedSortButton));
    }

    async getCreateTaskDropdownBody(){
        return await this.driver.findElement(By.css(createTaskDropdownBody));
    }

}

module.exports = tasksPage;

