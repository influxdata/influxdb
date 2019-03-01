import {Organization} from '@influxdata/influx'

describe('Tasks', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org).as('org')
    })

    cy.visit('/tasks')
  })

  it('can create a task', () => {
    const taskName = '🦄ask'
    cy.get('.empty-state').within(() => {
      cy.contains('Create').click()
    })

    cy.getByInputName('name').type(taskName)
    cy.getByInputName('interval').type('1d')
    cy.getByInputName('offset').type('20m')

    cy.getByTestID('flux-editor').within(() => {
      cy.get('textarea').type(
        `from(bucket: "defbuck")
      |> range(start: -2m)`,
        {force: true}
      )
    })

    cy.contains('Save').click()

    cy.getByTestID('task-row')
      .should('have.length', 1)
      .and('contain', taskName)
  })

  // TODO: wait on delete
  it.skip('can delete a task', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createTask(id)
      cy.createTask(id)
    })

    cy.getByTestID('task-row').should('have.length', 2)

    cy.getByTestID('confirmation-button')
      .first()
      .click({force: true})

    cy.wait(500)

    cy.getByTestID('task-row').should('have.length', 1)
  })

  it('fails to create a task without a valid script', () => {
    cy.get('.empty-state').within(() => {
      cy.contains('Create').click()
    })

    cy.getByInputName('name').type('🦄ask')
    cy.getByInputName('interval').type('1d')
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
        cy.createTask(id).then(({body}) => {
          cy.createLabel('tasks', body.id, newLabelName)
        })

        cy.createTask(id).then(({body}) => {
          cy.createLabel('tasks', body.id, 'bar')
        })
      })

      cy.visit('/tasks')

      cy.getByTestID('task-row').should('have.length', 2)

      cy.getByTestID(`label--pill ${newLabelName}`).click()

      cy.getByTestID('task-row').should('have.length', 1)
    })
  })

  describe('searching', () => {
    it('can search by task name', () => {
      const searchName = 'beepBoop'
      cy.get<Organization>('@org').then(({id}) => {
        cy.createTask(id, searchName)
        cy.createTask(id)
      })

      cy.visit('/tasks')

      cy.getByTestID('search-widget').type('bEE')

      cy.getByTestID('task-row').should('have.length', 1)
    })
  })
})
