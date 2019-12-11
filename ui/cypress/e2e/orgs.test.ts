describe('Orgs', () => {
  beforeEach(() => {
    cy.flush()
  })

  describe('when there is a user with no orgs', () => {
    beforeEach(() => {
      cy.signin().then(({body}) => {
        cy.deleteOrg(body.org.id)
      })

      cy.visit('/')
    })

    it('forwards the user to the No Orgs Page', () => {
      cy.url().should('contain', 'no-org')
    })
  })
})
