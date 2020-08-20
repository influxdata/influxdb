import {Organization, Bucket} from '../../src/types'

describe('Tasks', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org).as('org')
      cy.wrap(body.bucket).as('bucket')

      cy.createToken(body.org.id, 'test token', 'active', [
        {action: 'write', resource: {type: 'views'}},
        {action: 'write', resource: {type: 'documents'}},
        {action: 'write', resource: {type: 'tasks'}},
      ]).then(({body}) => {
        cy.wrap(body.token).as('token')
      })
    })

    cy.fixture('routes').then(({orgs}) => {
      cy.get('@org').then(({id}: Organization) => {
        cy.visit(`${orgs}/${id}/tasks`)
      })
    })
  })

  it('cannot create a task with an invalid to() function', () => {
    const taskName = 'Bad Task'

    createFirstTask(taskName, ({name}) => {
      return `import "influxdata/influxdb/v1{rightarrow}
v1.tagValues(bucket: "${name}", tag: "_field"{rightarrow}
from(bucket: "${name}"{rightarrow}
  |> range(start: -2m{rightarrow}
  |> to(org: "${name}"{rightarrow}`
    })

    cy.getByTestID('task-save-btn').click()

    cy.getByTestID('notification-error').should(
      'contain',
      'error calling function "to": missing required keyword argument "bucketID"'
    )
  })

  it('can create a task', () => {
    const taskName = 'Task'
    createFirstTask(taskName, ({name}) => {
      return `import "influxdata/influxdb/v1{rightarrow}
v1.tagValues(bucket: "${name}", tag: "_field"{rightarrow}
from(bucket: "${name}"{rightarrow}
   |> range(start: -2m{rightarrow}`
    })

    cy.getByTestID('task-save-btn').click()

    cy.getByTestID('notification-success--dismiss').click()

    cy.getByTestID('task-card')
      .should('have.length', 1)
      .and('contain', taskName)

    // TODO: extend to create from template overlay
    cy.getByTestID('add-resource-dropdown--button').click()
    cy.getByTestID('add-resource-dropdown--template').click()
    cy.getByTestID('task-import-template--overlay').within(() => {
      cy.get('.cf-overlay--dismiss').click()
    })

    // TODO: extend to create a template from JSON
    cy.getByTestID('add-resource-dropdown--button').click()
    cy.getByTestID('add-resource-dropdown--import').click()
    cy.getByTestID('task-import--overlay').within(() => {
      cy.get('.cf-overlay--dismiss').click()
    })
  })
  // this test is broken due to a failure on the post route
  it.skip('can create a task using http.post', () => {
    const taskName = 'Task'
    createFirstTask(taskName, () => {
      return `import "http{rightarrow}
http.post(
  url: "https://foo.bar/baz",
  data: bytes(v: "body"{rightarrow}
  {rightarrow}`
    })

    cy.getByTestID('task-save-btn').click()

    cy.getByTestID('task-card')
      .should('have.length', 1)
      .and('contain', taskName)
  })

  it('keeps user input in text area when attempting to import invalid JSON', () => {
    cy.getByTestID('page-control-bar').within(() => {
      cy.getByTestID('add-resource-dropdown--button').click()
    })

    cy.getByTestID('add-resource-dropdown--import').click()
    cy.contains('Paste').click()
    cy.getByTestID('import-overlay--textarea')
      .click()
      .type('this is invalid JSON')
    cy.get('button[title*="Import JSON"]').click()
    cy.getByTestID('import-overlay--textarea--error').should('have.length', 1)
    cy.getByTestID('import-overlay--textarea').should($s =>
      expect($s).to.contain('this is invalid JSON')
    )
    cy.getByTestID('import-overlay--textarea').type(
      '{backspace}{backspace}{backspace}{backspace}{backspace}'
    )
    cy.get('button[title*="Import JSON"]').click()
    cy.getByTestID('import-overlay--textarea--error').should('have.length', 1)
    cy.getByTestID('import-overlay--textarea').should($s =>
      expect($s).to.contain('this is invalid')
    )
  })

  it('can create a cron task', () => {
    cy.getByTestID('empty-tasks-list').within(() => {
      cy.getByTestID('add-resource-dropdown--button').click()
    })

    cy.getByTestID('add-resource-dropdown--new').click()

    cy.getByTestID('flux-editor').within(() => {
      cy.get('textarea.inputarea')
        .click()
        .type('from(bucket: "defbuck")\n' + '\t|> range(start: -2m)', {
          force: true,
          delay: 2,
        })
    })

    cy.getByTestID('task-form-name')
      .click()
      .type('Cron task test')
      .then(() => {
        cy.getByTestID('task-card-cron-btn').click()
        cy.getByTestID('task-form-schedule-input')
          .click()
          .type('0 4 8-14 * *')
        cy.getByTestID('task-form-offset-input')
          .click()
          .type('10m')
      })

    cy.getByTestID('task-save-btn').click()

    cy.getByTestID('task-card')
      .contains('Cron task test')
      .get('.cf-resource-meta--item')
      .contains('Scheduled to run 0 4 8-14 * *')

    cy.getByTestID('task-card')
      .contains('Cron task test')
      .click()
      .then(() => {
        cy.getByTestID('task-form-schedule-input').should(
          'have.value',
          '0 4 8-14 * *'
        )
      })
  })

  //skip until this problem is resolved
  it.skip('can create a task with an option parameter', () => {
    cy.getByTestID('empty-tasks-list').within(() => {
      cy.getByTestID('add-resource-dropdown--button')
        .click()
        .then(() => {
          cy.getByTestID('add-resource-dropdown--new').click()
        })
    })

    cy.getByTestID('flux-editor').within(() => {
      cy.get('textarea.inputarea')
        .click()
        .type(
          'option task = \n' +
            '{\n' +
            'name: "Option Test", \n' +
            'every: 24h, \n' +
            'offset: 20m\n' +
            '}\n' +
            'from(bucket: "defbuck")\n' +
            '\t|> range(start: -2m)'
        )
    })

    cy.getByTestID('task-form-name')
      .click()
      .type('Option Test')
      .then(() => {
        cy.getByTestID('task-form-schedule-input')
          .click()
          .type('24h')
        cy.getByTestID('task-form-offset-input')
          .click()
          .type('20m')
      })

    cy.getByTestID('task-save-btn').click()
    cy.getByTestID('notification-success').should('exist')
  })

  describe('When tasks already exist', () => {
    beforeEach(() => {
      cy.get('@org').then(({id}: Organization) => {
        cy.get<string>('@token').then(token => {
          cy.createTask(token, id)
        })
      })
      cy.reload()
    })

    it('can edit a task', () => {
      // Disabling the test
      cy.getByTestID('task-card--slide-toggle')
        .should('have.class', 'active')
        .then(() => {
          cy.getByTestID('task-card--slide-toggle')
            .click()
            .then(() => {
              cy.getByTestID('task-card--slide-toggle').should(
                'not.have.class',
                'active'
              )
            })
        })

      // Editing a name
      const newName = 'Task'

      cy.getByTestID('task-card').then(() => {
        cy.getByTestID('task-card--name')
          .trigger('mouseover')
          .then(() => {
            cy.getByTestID('task-card--name-button')
              .click()
              .then(() => {
                cy.getByTestID('task-card--input')
                  .type(newName)
                  .type('{enter}')
              })

            cy.getByTestID('notification-success').should('exist')
            cy.contains(newName).should('exist')
          })
      })

      // Add a label
      cy.getByTestID('task-card').within(() => {
        cy.getByTestID('inline-labels--add').click()
      })

      const labelName = 'l1'
      cy.getByTestID('inline-labels--popover--contents').type(labelName)
      cy.getByTestID('inline-labels--create-new').click()
      cy.getByTestID('create-label-form--submit').click()

      // Delete the label
      cy.getByTestID(`label--pill--delete ${labelName}`).click({force: true})
      cy.getByTestID('inline-labels--empty').should('exist')
    })

    it('can delete a task', () => {
      cy.getByTestID('task-card')
        .first()
        .trigger('mouseover')
        .then(() => {
          cy.getByTestID('context-delete-menu')
            .click()
            .then(() => {
              cy.getByTestID('context-delete-task')
                .click()
                .then(() => {
                  cy.getByTestID('empty-tasks-list').should('exist')
                })
            })
        })
    })

    it('can clone a task and edit it', () => {
      //clone a task
      cy.getByTestID('task-card')
        .first()
        .trigger('mouseover')
        .then(() => {
          cy.get('.context-menu--container')
            .eq(1)
            .within(() => {
              cy.getByTestID('context-menu')
                .click()
                .then(() => {
                  cy.getByTestID('context-menu-item')
                    .contains('Clone')
                    .click()
                })
            })
        })

      cy.getByTestID('task-card').should('have.length', 2)

      //assert the values of the task and change them
      cy.getByTestID('task-card--name')
        .eq(1)
        .click()
        .then(() => {
          cy.getByTestID('task-form-name')
            .should('have.value', '🦄ask')
            .then(() => {
              cy.getByTestID('task-form-name')
                .click()
                .clear()
                .type('Copy task test')
                .then(() => {
                  cy.getByTestID('task-form-schedule-input')
                    .should('have.value', '24h')
                    .clear()
                    .type('12h')
                    .should('have.value', '12h')
                  cy.getByTestID('task-form-offset-input')
                    .should('have.value', '20m')
                    .clear()
                    .type('10m')
                    .should('have.value', '10m')
                  cy.getByTestID('task-save-btn').click()
                })
            })
        })

      //assert changed task name
      cy.getByTestID('task-card--name').contains('Copy task test')
    })

    //skip until this problem is resolved
    it.skip('can add a comment into a task', () => {
      cy.getByTestID('task-card--name')
        .first()
        .click()

      //assert textarea and write a comment
      cy.getByTestID('flux-editor').within(() => {
        cy.get('textarea.inputarea')
          .should(
            'have.value',
            'option task = {\n' +
              '    name: "🦄ask",\n' +
              '    every: 24h,\n' +
              '    offset: 20m\n' +
              '  }\n' +
              '  from(bucket: "defbuck")\n' +
              '        |> range(start: -2m)'
          )
          .click()
          .focused()
          .type('{end} {enter} //this is a test comment')
          .then(() => {
            cy.get('textarea.inputarea').should(
              'have.value',
              'option task = {\n' +
                '    name: "🦄ask",\n' +
                '    every: 24h,\n' +
                '    offset: 20m\n' +
                '  }\n' +
                '  from(bucket: "defbuck")\n' +
                '        |> range(start: -2m) \n' +
                '         //this is a test comment'
            )
          })
      })

      //save and assert notification
      cy.getByTestID('task-save-btn')
        .click()
        .then(() => {
          cy.getByTestID('notification-success').contains(
            'Task was updated successfully'
          )
        })

      cy.getByTestID('task-card--name')
        .first()
        .click()

      //assert the comment
      cy.getByTestID('flux-editor').within(() => {
        cy.get('textarea.inputarea').should(
          'have.value',
          'option task = {name: "🦄ask", every: 24h, offset: 20m}\n' +
            '\n' +
            'from(bucket: "defbuck")\n' +
            '\t|> range(start: -2m)\n' +
            '    //this is a test comment'
        )
      })
    })
  })

  describe('Searching and filtering', () => {
    const newLabelName = 'click-me'
    const taskName = 'beepBoop'

    beforeEach(() => {
      cy.get('@org').then(({id}: Organization) => {
        cy.get<string>('@token').then(token => {
          cy.createTask(token, id, taskName).then(({body}) => {
            cy.createAndAddLabel('tasks', id, body.id, newLabelName)
          })

          cy.createTask(token, id).then(({body}) => {
            cy.createAndAddLabel('tasks', id, body.id, 'bar')
          })
        })
      })

      cy.fixture('routes').then(({orgs}) => {
        cy.get('@org').then(({id}: Organization) => {
          cy.visit(`${orgs}/${id}/tasks`)
        })
      })
    })

    it('can click to filter tasks by labels', () => {
      cy.getByTestID('task-card').should('have.length', 2)

      cy.getByTestID(`label--pill ${newLabelName}`).click()

      cy.getByTestID('task-card').should('have.length', 1)

      // searching by task name
      cy.getByTestID('search-widget')
        .clear()
        .type('bEE')

      cy.getByTestID('task-card').should('have.length', 1)
    })
  })

  describe('update & persist data', () => {
    // address a bug that was reported when editing tasks:
    // https://github.com/influxdata/influxdb/issues/15534
    const taskName = 'Task'
    const interval = '12h'
    const offset = '30m'
    beforeEach(() => {
      createFirstTask(
        taskName,
        ({name}) => {
          return `import "influxdata/influxdb/v1{rightarrow}
  v1.tagValues(bucket: "${name}", tag: "_field"{rightarrow}
  from(bucket: "${name}"{rightarrow}
    |> range(start: -2m{rightarrow}`
        },
        interval,
        offset
      )
      cy.getByTestID('task-save-btn').click()
      cy.getByTestID('task-card')
        .should('have.length', 1)
        .and('contain', taskName)

      cy.getByTestID('task-card--name')
        .contains(taskName)
        .click()
      // verify that the previously input data exists
      cy.getByInputValue(taskName)
      cy.getByInputValue(interval)
      cy.getByInputValue(offset)
    })

    it('can update a task', () => {
      const newTask = 'Taskr[sic]'
      const newInterval = '24h'
      const newOffset = '7h'
      // updates the data
      cy.getByTestID('task-form-name')
        .clear()
        .type(newTask)
      cy.getByTestID('task-form-schedule-input')
        .clear()
        .type(newInterval)
      cy.getByTestID('task-form-offset-input')
        .clear()
        .type(newOffset)

      cy.getByTestID('task-save-btn').click()
      // checks to see if the data has been updated once saved
      cy.getByTestID('task-card--name').contains(newTask)
    })

    it('persists data when toggling between scheduling tasks', () => {
      // toggles schedule task from every to cron
      cy.getByTestID('task-card-cron-btn').click()

      // checks to see if the cron helper text exists
      cy.getByTestID('form--box').should('have.length', 1)

      const cronInput = '0 2 * * *'
      // checks to see if the cron data is set to the initial value
      cy.getByInputValue('')
      cy.getByInputValue(offset)

      cy.getByTestID('task-form-schedule-input').type(cronInput)
      // toggles schedule task back to every from cron
      cy.getByTestID('task-card-every-btn').click()
      // checks to see if the initial interval data for every persists
      cy.getByInputValue(interval)
      cy.getByInputValue(offset)
      // toggles back to cron from every
      cy.getByTestID('task-card-cron-btn').click()
      // checks to see if the cron data persists
      cy.getByInputValue(cronInput)
      cy.getByInputValue(offset)
      cy.getByTestID('task-save-btn').click()
    })
  })

  describe('renders the correct name when toggling between tasks', () => {
    // addresses an issue that was reported when clicking tasks
    // this issue could not be reproduced manually | testing:
    // https://github.com/influxdata/influxdb/issues/15552
    const firstTask = 'First_Task'
    const secondTask = 'Second_Task'
    beforeEach(() => {
      cy.get('@org').then(({id}: Organization) => {
        cy.get<string>('@token').then(token => {
          cy.createTask(token, id, firstTask)
          cy.createTask(token, id, secondTask)
        })
      })

      cy.fixture('routes').then(({orgs}) => {
        cy.get('@org').then(({id}: Organization) => {
          cy.visit(`${orgs}/${id}/tasks`)
        })
      })
    })

    it('when navigating using the navbar', () => {
      // click on the second task
      cy.getByTestID('task-card--name')
        .contains(secondTask)
        .click()
      // verify that it is the correct data
      cy.getByInputValue(secondTask)

      cy.get('.cf-tree-nav--item__active').within(() => {
        // Get the element that has a click handler within the nav item
        cy.get('.cf-tree-nav--item-block').click()
      })
      // navigate back to the first one to verify that the name is correct
      cy.getByTestID('task-card--name')
        .contains(firstTask)
        .click()
      cy.getByInputValue(firstTask)
    })

    it('when navigating using the cancel button', () => {
      // click on the second task
      cy.getByTestID('task-card--name')
        .contains(secondTask)
        .click()
      // verify that it is the correct data
      cy.getByInputValue(secondTask)
      cy.getByTestID('task-cancel-btn').click()
      // navigate back to the first task again
      cy.getByTestID('task-card--name')
        .contains(firstTask)
        .click()
      cy.getByInputValue(firstTask)
      cy.getByTestID('task-cancel-btn').click()
    })

    it('when navigating using the save button', () => {
      // click on the second task
      cy.getByTestID('task-card--name')
        .contains(secondTask)
        .click()
      // verify that it is the correct data
      cy.getByInputValue(secondTask)
      cy.getByTestID('task-save-btn').click()
      // navigate back to the first task again
      cy.getByTestID('task-card--name')
        .contains(firstTask)
        .click()
      cy.getByInputValue(firstTask)
      cy.getByTestID('task-save-btn').click()
    })
  })
})

function createFirstTask(
  name: string,
  flux: (bucket: Bucket) => string,
  interval: string = '24h',
  offset: string = '20m'
) {
  cy.getByTestID('empty-tasks-list').within(() => {
    cy.getByTestID('add-resource-dropdown--button').click()
  })

  cy.getByTestID('add-resource-dropdown--new').click()

  cy.get<Bucket>('@bucket').then(bucket => {
    cy.getByTestID('flux-editor').within(() => {
      cy.get('textarea.inputarea')
        .click()
        .focused()
        .type(flux(bucket), {force: true, delay: 2})
    })
  })

  cy.getByInputName('name').type(name)
  cy.getByTestID('task-form-schedule-input').type(interval)
  cy.getByTestID('task-form-offset-input').type(offset)
}
