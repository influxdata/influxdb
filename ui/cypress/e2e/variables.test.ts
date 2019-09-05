import {Organization} from '../../src/types'

describe('Variables', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org).as('org')
      cy.visit(`orgs/${body.org.id}/settings/variables`)
    })
  })

  it('can create a variable', () => {
    cy.get('.cf-empty-state').within(() => {
      cy.contains('Create').click()
    })

    cy.getByTestID('add-resource-dropdown--new').click()

    cy.getByInputName('name').type('Little Variable')
    cy.getByTestID('flux-editor').within(() => {
      cy.get('textarea').type('filter(fn: (r) => r._field == "cpu")', {
        force: true,
      })
    })

    cy.get('form')
      .contains('Create')
      .click()

    cy.getByTestID('resource-card').should('have.length', 1)
  })

  it.skip('can delete a variable', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createVariable(id)
      cy.createVariable(id)
    })

    cy.getByTestID('resource-card').should('have.length', 2)

    cy.getByTestID('confirmation-button')
      .first()
      .click({force: true})

    cy.getByTestID('resource-card').should('have.length', 1)
  })
})
