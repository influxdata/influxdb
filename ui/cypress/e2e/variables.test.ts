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

  it('keeps user input in text area when attempting to import invalid JSON', () => {
    cy.get('.tabbed-page-section--header').within(() => {
      cy.contains('Create').click()
    })

    cy.getByTestID('add-resource-dropdown--import').click()
    cy.contains('Paste').click()
    cy.getByTestID('import-overlay--textarea')
      .click()
      .type('this is invalid JSON')
    cy.get('button[title*="Import JSON"]').click()
    cy.getByTestID('import-overlay--textarea--error').should('have.length', 1)
    cy.getByTestID('import-overlay--textarea').should($s =>
      expect($s).to.contain('this is invalid JSON')
    )
    cy.getByTestID('import-overlay--textarea').type(
      '{backspace}{backspace}{backspace}{backspace}{backspace}'
    )
    cy.get('button[title*="Import JSON"]').click()
    cy.getByTestID('import-overlay--textarea--error').should('have.length', 1)
    cy.getByTestID('import-overlay--textarea').should($s =>
      expect($s).to.contain('this is invalid')
    )
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
