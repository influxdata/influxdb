describe('Variables', () => {
  beforeEach(() => {
    cy.flush()

    cy.setupUser().then(({body}) => {
      cy.signin(body.org.id)

      cy.wrap(body.org).as('org')
      cy.visit(`organizations/${body.org.id}/variables_tab`)
    })
  })

  it('can create a variable', () => {
    cy.get('.empty-state').within(() => {
      cy.contains('Create').click()
    })

    cy.getByInputName('name').type('Little Variable')
    cy.getByDataTest('flux-editor').within(() => {
      cy.get('textarea').type('filter(fn: (r) => r._field == "cpu")', {
        force: true,
      })
    })

    cy.get('form')
      .contains('Create')
      .click()

    cy.getByDataTest('variable-row').should('have.length', 1)
  })
})
