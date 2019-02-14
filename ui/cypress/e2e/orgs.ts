describe('Orgs', () => {
  beforeEach(() => {
    cy.flush()

    cy.setupUser()

    cy.signin()

    cy.visit('/organizations')
  })

  it.only('can create an org', () => {
    cy.get('.index-list--row')
      .its('length')
      .should('be.eq', 1)

    cy.get('.page-header--right > .button')
      .contains('Create')
      .click()

    const orgName = '🅱️organization'
    cy.getByInputName('name').type(orgName)

    cy.getByTitle('Create').click()

    cy.get('.index-list--row')
      .should('contain', orgName)
      .its('length')
      .should('be.eq', 2)
  })
})
