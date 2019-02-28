describe('Collectors', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')

      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/telegrafs`)
      })
    })
  })

  describe('from the org view', () => {
    it('can create a collector', () => {
      const newCollector = 'New Collector'
      const collectorDescription = 'This is a new collector testing'

      cy.getByTestID('table-row').should('have.length', 0)

      cy.contains('Create Configuration').click()
      cy.getByTestID('overlay--container').within(() => {
        cy.getByInputName('System').click({force: true})
        cy.get('.button')
          .contains('Continue')
          .click()
        cy.getByInputName('name')
          .clear()
          .type(newCollector)
        cy.getByInputName('description')
          .clear()
          .type(collectorDescription)
        cy.get('.button')
          .contains('Create and Verify')
          .click()
        cy.getByTestID('streaming').within(() => {
          cy.get('.button')
            .contains('Listen for Data')
            .click()
        })
        cy.get('.button')
          .contains('Finish')
          .click()
      })

      cy.getByTestID('table-row')
        .should('have.length', 1)
        .and('contain', newCollector)
    })
  })
})
