import {Organization} from '../../src/types'

describe('Variables', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org).as('org')
      cy.createQueryVariable(body.org.id)
      cy.visit(`orgs/${body.org.id}/settings/variables`)
    })
  })

  it('can create a variable', () => {
    cy.getByTestID('resource-card').should('have.length', 1)
    // ensure that the default variables are not accessible on the Variables Tab
    cy.getByTestID('resource-card').should('not.contain', 'timeRangeStart')
    cy.getByTestID('resource-card').should('not.contain', 'timeRangeStop')
    cy.getByTestID('resource-card').should('not.contain', 'windowPeriod')

    cy.getByTestID('add-resource-dropdown--button').click()

    cy.getByTestID('add-resource-dropdown--new').should('have.length', 1)

    cy.getByTestID('add-resource-dropdown--new').click()

    cy.getByInputName('name').type('a Second Variable')
    cy.getByTestID('flux-editor').within(() => {
      cy.get('.react-monaco-editor-container')
        .click()
        .focused()
        .type('filter(fn: (r) => r._field == "cpu")', {
          force: true,
        })
    })

    cy.get('form')
      .contains('Create')
      .click()

    cy.getByTestID('resource-card').should('have.length', 2)
  })

  it('keeps user input in text area when attempting to import invalid JSON', () => {
    cy.getByTestID('tabbed-page--header').within(() => {
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

  it('can delete a variable', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createQueryVariable(id, 'anotherVariable')
    })

    cy.getByTestID('resource-card').should('have.length', 2)

    cy.getByTestID('context-delete-menu')
      .first()
      .click({force: true})
    cy.getByTestID('context-delete-variable')
      .first()
      .click({force: true})

    cy.getByTestID('resource-card').should('have.length', 1)
  })

  it('can edit a variable', () => {
    cy.getByTestID('resource-card').should('have.length', 1)

    cy.getByTestID('context-menu')
      .first()
      .click({force: true})

    cy.getByTestID('context-rename-variable').click({force: true})

    cy.getByTestID('danger-confirmation-button').click()

    cy.getByInputName('name').type('-renamed')

    cy.getByTestID('rename-variable-submit').click()

    cy.get('.cf-resource-name--text').should($s =>
      expect($s).to.contain('-renamed')
    )

    // Create a label
    cy.getByTestID('resource-card').within(() => {
      cy.getByTestID('inline-labels--add').click()
    })

    const labelName = 'l1'
    cy.getByTestID('inline-labels--popover--contents').type(labelName)
    cy.getByTestID('inline-labels--create-new').click()
    cy.getByTestID('create-label-form--submit').click()

    // Delete the label
    cy.getByTestID(`label--pill--delete ${labelName}`).click({force: true})
    cy.getByTestID('inline-labels--empty').should('exist')
  })
})
