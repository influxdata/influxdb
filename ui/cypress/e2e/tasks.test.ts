import {Organization, Bucket} from '../../src/types'
import _ from 'lodash'

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
      cy.get<Organization>('@org').then(({id}) => {
        cy.visit(`${orgs}/${id}/tasks`)
      })
    })
  })

  it('can create a task', () => {
    const taskName = 'Task'
    cy.getByTestID('empty-tasks-list').within(() => {
      cy.getByTestID('add-resource-dropdown--button').click()
    })

    cy.getByTestID('add-resource-dropdown--new').click()

    cy.getByInputName('name').type(taskName)
    cy.getByInputName('interval').type('24h')
    cy.getByInputName('offset').type('20m')

    cy.get<Bucket>('@bucket').then(({name}) => {
      cy.getByTestID('flux-editor').within(() => {
        cy.get('textarea').type(
          `import "influxdata/influxdb/v1"\nv1.tagValues(bucket: "${name}", tag: "_field")\nfrom(bucket: "${name}")\n|> range(start: -2m)`,
          {force: true}
        )
      })

      cy.contains('Save').click()

      cy.getByTestID('task-card')
        .should('have.length', 1)
        .and('contain', taskName)
    })
  })

  it('can delete a task', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.get<string>('@token').then(token => {
        cy.createTask(token, id)
        cy.createTask(token, id)
      })

      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/tasks`)
      })

      cy.getByTestID('task-card').should('have.length', 2)

      cy.getByTestID('task-card')
        .first()
        .trigger('mouseover')
        .within(() => {
          cy.getByTestID('context-delete-menu').click()
          cy.getByTestID('context-delete-task').click()
        })

      cy.getByTestID('task-card').should('have.length', 1)
    })
  })

  it('can disable a task', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.get<string>('@token').then(token => {
        cy.createTask(token, id)
      })
    })

    cy.getByTestID('task-card--slide-toggle').should('have.class', 'active')

    cy.getByTestID('task-card--slide-toggle').click()

    cy.getByTestID('task-card--slide-toggle').should('not.have.class', 'active')
  })

  it('can edit a tasks name', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.get<string>('@token').then(token => {
        cy.createTask(token, id)
      })
    })

    const newName = 'Task'

    cy.getByTestID('task-card').within(() => {
      cy.getByTestID('task-card--name').trigger('mouseover')

      cy.getByTestID('task-card--name-button').click()

      cy.get('.cf-input-field')
        .type(newName)
        .type('{enter}')
    })

    cy.fixture('routes').then(({orgs}) => {
      cy.get<Organization>('@org').then(({id}) => {
        cy.visit(`${orgs}/${id}/tasks`)
      })
    })

    cy.getByTestID('task-card').should('contain', newName)
  })

  it('fails to create a task without a valid script', () => {
    cy.getByTestID('empty-tasks-list').within(() => {
      cy.getByTestID('add-resource-dropdown--button').click()
    })

    cy.getByTestID('add-resource-dropdown--new').click()

    cy.getByInputName('name').type('Task')
    cy.getByInputName('interval').type('24h')
    cy.getByInputName('offset').type('20m')

    cy.getByTestID('flux-editor').within(() => {
      cy.get('textarea').type('{}', {force: true})
    })

    cy.contains('Save').click()

    cy.getByTestID('notification-error').should('exist')
  })

  describe('labeling', () => {
    it('can click to filter tasks by labels', () => {
      const newLabelName = 'click-me'

      cy.get<Organization>('@org').then(({id}) => {
        cy.get<string>('@token').then(token => {
          cy.createTask(token, id).then(({body}) => {
            cy.createAndAddLabel('tasks', id, body.id, newLabelName)
          })

          cy.createTask(token, id).then(({body}) => {
            cy.createAndAddLabel('tasks', id, body.id, 'bar')
          })
        })
      })

      cy.fixture('routes').then(({orgs}) => {
        cy.get<Organization>('@org').then(({id}) => {
          cy.visit(`${orgs}/${id}/tasks`)
        })
      })

      cy.getByTestID('task-card').should('have.length', 2)

      cy.getByTestID(`label--pill ${newLabelName}`).click()

      cy.getByTestID('task-card').should('have.length', 1)
    })
  })

  describe('searching', () => {
    it('can search by task name', () => {
      const searchName = 'beepBoop'
      cy.get<Organization>('@org').then(({id}) => {
        cy.get<string>('@token').then(token => {
          cy.createTask(token, id, searchName)
          cy.createTask(token, id)
        })
      })

      cy.fixture('routes').then(({orgs}) => {
        cy.get<Organization>('@org').then(({id}) => {
          cy.visit(`${orgs}/${id}/tasks`)
        })
      })

      cy.getByTestID('search-widget').type('bEE')

      cy.getByTestID('task-card').should('have.length', 1)
    })
  })
})
