const orgRoute = '/organizations'

describe('Orgs', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin()

    cy.visit(orgRoute)
  })

  it('can create an org', () => {
    cy.get('.index-list--row')
      .its('length')
      .should('be.eq', 1)

    cy.getByTestID('create-org-button').click()

    const orgName = '🅱️organization'

    cy.getByTestID('create-org-name-input').type(orgName)

    cy.getByTestID('create-org-submit-button').click()

    cy.get('.index-list--row')
      .should('contain', orgName)
      .its('length')
      .should('be.eq', 2)
  })

  it('can delete an org', () => {
    cy.createOrg().then(() => {
      cy.get('.index-list--row').then(rows => {
        const numOrgs = rows.length

        cy.contains('Confirm').click({force: true})

        cy.get('.index-list--row')
          .its('length')
          .should('eq', numOrgs - 1)
      })
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
