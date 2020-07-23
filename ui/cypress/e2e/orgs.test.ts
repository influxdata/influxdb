const secondOrg = 'Second Org'
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

  describe('updating and switching orgs', () => {
    beforeEach(() => {
      cy.signin().then(() => {
        cy.createOrg(secondOrg)
        cy.visit('/')
      })
    })

    it('should be able to rename the org', () => {
      const extraText = '_my_renamed_org_in_e2e'
      cy.getByTestID('user-nav').click()
      cy.getByTestID('user-nav-item-about').click()
      cy.get('span:contains("About")').click()
      cy.getByTestID('rename-org--button').click()
      cy.getByTestID('danger-confirmation-button').click()
      cy.getByTestID('create-org-name-input')
        .click()
        .type(extraText)
      cy.get('button.cf-button.cf-button-success').click()
      cy.get('.cf-tree-nav--team')
        .contains(extraText)
        .should('have.length', 1)

      // Switch Orgs
      cy.getByTestID('user-nav').click()
      cy.getByTestID('user-nav-item-switch-orgs').click()
      cy.getByTestID('overlay--body').within(() => {
        cy.contains(secondOrg).click()
      })

      cy.getByTestID('user-nav')
        .click()
        .contains(secondOrg)
      cy.getByTestID('page').should('exist')
      cy.getByTestID('page-header').should('exist')
    })
  })
})
