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

  it('can create a CSV, map, and query variable', () => {
    cy.getByTestID('resource-card variable').should('have.length', 1)
    // ensure that the default variables are not accessible on the Variables Tab
    cy.getByTestID('resource-card variable').should(
      'not.contain',
      'timeRangeStart'
    )
    cy.getByTestID('resource-card variable').should(
      'not.contain',
      'timeRangeStop'
    )
    cy.getByTestID('resource-card variable').should(
      'not.contain',
      'windowPeriod'
    )

    cy.getByTestID('add-resource-dropdown--button').click()

    cy.getByTestID('add-resource-dropdown--new').should('have.length', 1)

    cy.getByTestID('add-resource-dropdown--new').click()

    cy.getByTestID('variable-type-dropdown--button').click()
    cy.getByTestID('variable-type-dropdown-constant').click()

    const variableName = 'a Second Variable'
    cy.getByInputName('name').type(variableName)

    cy.get('textarea').type('1,2,3,4,5,6')

    cy.getByTestID('csv-value-select-dropdown')
      .click()
      .contains('6')
      .click()

    cy.get('form')
      .contains('Create')
      .click()

    cy.getByTestID(`variable-card--name ${variableName}`).click()
    cy.getByTestID('notification-success--dismiss').click()

    cy.getByTestID('edit-variable--overlay').within(() => {
      cy.getByTestID('variable-type-dropdown--button').click()
      cy.getByTestID('variable-type-dropdown-query').click()

      cy.getByTestID('flux-editor').within(() => {
        cy.get('.react-monaco-editor-container')
          .click()
          .focused()
          .type('filter(fn: (r) => r._field == "cpu")', {
            force: true,
          })
      })

      cy.get('form')
        .contains('Submit')
        .click()
    })

    cy.getByTestID('notification-success--dismiss').click()
    cy.getByTestID(`variable-card--name ${variableName}`).click()

    cy.getByTestID('edit-variable--overlay').within(() => {
      cy.getByTestID('variable-type-dropdown--button').click()
      cy.getByTestID('variable-type-dropdown-map').click()

      const lastMapItem = 'Mila Emile,"o61AhpOGr5aO3cYVArC0"'
      cy.get('textarea').type(`Juanito MacNeil,"5TKl6l8i4idg15Fxxe4P"
      Astrophel Chaudhary,"bDhZbuVj5RV94NcFXZPm"
      Ochieng Benes,"YIhg6SoMKRUH8FMlHs3V"
      ${lastMapItem}`)

      cy.getByTestID('map-variable-dropdown--button').click()
      cy.contains(lastMapItem).click()

      cy.get('form')
        .contains('Submit')
        .click()
    })

    cy.getByTestID('notification-success--dismiss').click()

    cy.getByTestID('resource-card variable').should('have.length', 2)
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
    cy.getByTestID('import-overlay--textarea').contains('this is invalid')
  })

  it('can delete a variable', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createQueryVariable(id, 'anotherVariable')
    })

    cy.getByTestID('resource-card variable').should('have.length', 2)

    cy.getByTestID('context-delete-menu')
      .first()
      .click({force: true})
    cy.getByTestID('context-delete-variable')
      .first()
      .click({force: true})

    cy.getByTestID('notification-success--dismiss').click()

    cy.getByTestID('resource-card variable').should('have.length', 1)
  })

  it('can edit a variable and add a label', () => {
    cy.getByTestID('resource-card variable').should('have.length', 1)

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
    cy.getByTestID('resource-card variable').within(() => {
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
