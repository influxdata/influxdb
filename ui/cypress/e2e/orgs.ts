const orgRoute = '/organizations'

describe('Orgs', () => {
  beforeEach(() => {
    cy.flush()

    cy.setupUser()

    cy.signin()

    cy.visit(orgRoute)
  })

  it('can create an org', () => {
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

  it('can delete an org', () => {
    cy.createOrg()

    cy.get('.index-list--row').then(rows => {
      const numOrgs = rows.length

      cy.contains('Confirm').click({force: true})

      cy.get('.index-list--row')
        .its('length')
        .should('eq', numOrgs - 1)
    })
  })

  it('can update an org name', () => {
    cy.createOrg().then(({body}) => {
      const newName = 'new 🅱️organization'
      cy.visit(`${orgRoute}/${body.id}/member_tab`)

      cy.get('.renamable-page-title--title').click()
      cy.get('.input-field')
        .type(newName)
        .type('{enter}')

      cy.visit('/organizations')

      cy.get('.index-list--row').should('contain', newName)
    })
  })
})
