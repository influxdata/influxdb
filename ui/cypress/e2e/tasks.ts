describe('Tasks', () => {
  beforeEach(() => {
    cy.flush()

    cy.setupUser().then(({body}) => {
      cy.signin(body.org.id)
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

    cy.getByDataTest('flux-editor').within(() => {
      cy.get('textarea').type(
        `from(bucket: "default")
      |> range(start: -2m)`,
        {force: true}
      )
    })

    cy.contains('Save').click()

    cy.getByDataTest('task-row')
      .and('have.length', 1)
      .and('contain', taskName)
  })

  it('fails to create a task without a valid script', () => {
    cy.get('.empty-state').within(() => {
      cy.contains('Create').click()
    })

    cy.getByInputName('name').type('🦄ask')
    cy.getByInputName('interval').type('1d')
    cy.getByInputName('offset').type('20m')

    cy.getByDataTest('flux-editor').within(() => {
      cy.get('textarea').type('{}', {force: true})
    })

    cy.contains('Save').click()

    cy.getByDataTest('notification-error').should('exist')
  })
})
