const orgRoute = '/organizations'

describe('Orgs', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin()

    cy.visit(orgRoute)
  })

  for (let i = 0; i < 75; i++) {
    it('can create an org', () => {
      cy.get('.index-list--row').should('have.length', 1)

      cy.getByTestID('create-org-button').click()

      const orgName = '🅱️organization'

      cy.getByTestID('create-org-name-input').type(orgName)

      cy.getByTestID('create-org-submit-button').click()

      cy.get('.index-list--row')
        .should('contain', orgName)
        .and('have.length', 2)
    })

    it('can delete an org', () => {
      cy.createOrg()

      cy.visit(orgRoute)

      cy.getByTestID('table-row').should('have.length', 2)

      cy.getByTestID('table-row')
        .last()
        .trigger('mouseover')
        .within(() => {
          cy.getByTestID('delete-button')
            .trigger('mouseover')
            .click()

          cy.getByTestID('confirmation-button').click()
        })

      cy.getByTestID('table-row').should('have.length', 1)
    })

    //TODO: skipping update an org name because it is flaky but needs fixing: https://github.com/influxdata/influxdb/issues/12311
    it('can update an org name', () => {
      cy.createOrg().then(({body}) => {
        const newName = 'new 🅱️organization'
        cy.visit(`${orgRoute}/${body.id}/members`)

        cy.get('.renamable-page-title--title').click()

        cy.getByTestID('page-header').within(() => {
          cy.getByTestID('input-field')
            .type(newName)
            .type('{enter}')
        })

        cy.visit('/organizations')

        cy.get('.index-list--row').should('contain', newName)
      })
    })
  }
})
