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
      cy.contains('Sign In').click()
      cy.url().should('contain', 'signin')
    })
  })

  describe('when user wants to rename an org', () => {
    beforeEach(() => {
      cy.signin().then(({body}) => {
        console.log('signed in, body is', body)
      })

      cy.visit('/')
    })
    it('should be able to rename the org', () => {
      const extraText = '_renamed'
      cy.getByTestID('nav-item-org').click()
      cy.get('span:contains("About")').click()
      cy.get('span:contains("Rename")').click()
      cy.get('button.cf-button.cf-button-danger').click()
      cy.getByTestID('create-org-name-input')
        .click()
        .type(extraText)
      cy.get('button.cf-button.cf-button-success').click()
      cy.get('.cf-tree-nav--team')
        .contains(extraText)
        .should('have.length', 1)
    })
  })
})
