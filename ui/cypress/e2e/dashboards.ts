describe('Dashboards', () => {
  beforeEach(() => {
    // cy.flush() TODO: temporarily commented out until inmem data store is fixed

    cy.tempSignin()

    cy.visit('/dashboards')
  })

  it('can create a dashboard', () => {
    cy.get('.page-header--right > .button')
      .contains('Create')
      .click()

    cy.contains('Add Cell').should('exist')

    cy.visit('/dashboards')

    cy.get('.index-list--body')
      .children()
      .should(list => {
        expect(list.length).to.be.above(0)
      })
  })

  it('can delete a dashboard', () => {
    cy.get('.page-header--right > .button')
      .contains('Create')
      .click()

    cy.visit('/dashboards')

    cy.get('tbody').then(tbody => {
      const numDashboards = tbody.contents().length

      cy.get('.button-danger')
        .first()
        .click()

      cy.contains('Confirm')
        .first()
        .click({force: true})

      cy.get('.index-list--row')
        .its('length')
        .should('eq', numDashboards - 1)
    })
  })
})
