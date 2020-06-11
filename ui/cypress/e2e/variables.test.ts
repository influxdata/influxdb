describe('Variables', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org).as('org')
      cy.createQueryVariable(body.org.id)
      cy.visit(`orgs/${body.org.id}/settings/variables`)
    })
  })

  it('can CRUD a CSV, map, and query variable and search for variables based on names', () => {
    // Navigate away from and back to variables index using the nav bar
    cy.getByTestID('nav-item-dashboards').click()
    cy.getByTestID('nav-item-settings').click()
    cy.getByTestID('templates--tab').click()
    cy.getByTestID('variables--tab').click()

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

    cy.getByTestID('add-resource-dropdown--new').click()

    cy.getByTestID('variable-type-dropdown--button').click()
    cy.getByTestID('variable-type-dropdown-constant').click()

    // Create a CSV variable
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

    // Change it to a Query variable
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

    // Change it to a Map variable
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

    // Search variable by name
    cy.getByTestID('search-widget').type(variableName)

    cy.getByTestID('resource-card variable')
      .should('have.length', 1)
      .contains(variableName)

    // Delete a variable
    cy.getByTestID('context-delete-menu')
      .first()
      .click({force: true})
    cy.getByTestID('context-delete-variable')
      .first()
      .click({force: true})

    cy.getByTestID('notification-success--dismiss').click()

    cy.getByTestID('search-widget').clear()

    cy.getByTestID('resource-card variable')
      .should('have.length', 1)
      .contains('Little Variable')

    // Rename the variable
    cy.getByTestID('context-menu')
      .first()
      .click({force: true})

    cy.getByTestID('context-rename-variable').click({force: true})

    cy.getByTestID('danger-confirmation-button').click()

    cy.getByInputName('name').type('-renamed')

    cy.getByTestID('rename-variable-submit').click()

    cy.getByTestID('notification-success--dismiss').click()

    cy.getByTestID(`variable-card--name Little Variable-renamed`).should(
      'exist'
    )

    // Create a Map variable from scratch
    cy.getByTestID('add-resource-dropdown--button').click()

    cy.getByTestID('add-resource-dropdown--new').click()

    cy.getByTestID('variable-type-dropdown--button').click()
    cy.getByTestID('variable-type-dropdown-map').click()

    const mapVariableName = 'Map Variable'
    cy.getByInputName('name').type(mapVariableName)

    cy.get('textarea').type(`Astrophel Chaudhary,"bDhZbuVj5RV94NcFXZPm"
      Ochieng Benes,"YIhg6SoMKRUH8FMlHs3V"`)

    cy.getByTestID('map-variable-dropdown--button')
      .click()
      .last()
      .click()

    cy.get('form')
      .contains('Create')
      .click()

    cy.getByTestID('notification-success--dismiss').click()
    cy.getByTestID(`variable-card--name ${mapVariableName}`).should('exist')

    // Create a Query variable from scratch
    cy.getByTestID('add-resource-dropdown--button').click()

    cy.getByTestID('add-resource-dropdown--new').click()

    cy.getByTestID('variable-type-dropdown--button').click()
    cy.getByTestID('variable-type-dropdown-map').click()

    const queryVariableName = 'Query Variable'
    cy.getByInputName('name').type(queryVariableName)

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
      .contains('Create')
      .click()

    cy.getByTestID('notification-success--dismiss').click()
    cy.getByTestID(`variable-card--name ${queryVariableName}`).should('exist')
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

  it('can create and delete a label and filter a variable by label name & sort by variable name', () => {
    cy.getByTestID('resource-card variable').within(() => {
      cy.getByTestID('inline-labels--add').click()
    })

    const labelName = 'label'
    cy.getByTestID('inline-labels--popover--contents').type(labelName)
    cy.getByTestID('inline-labels--create-new').click()
    cy.getByTestID('create-label-form--submit').click()

    cy.getByTestID('add-resource-dropdown--button').click()

    cy.getByTestID('add-resource-dropdown--new').should('have.length', 1)

    cy.getByTestID('add-resource-dropdown--new').click()

    cy.getByTestID('variable-type-dropdown--button').click()
    cy.getByTestID('variable-type-dropdown-constant').click()

    // Create a CSV variable
    const variableName = 'a Second Variable'
    const defaultVar = 'Little Variable'
    cy.getByInputName('name').type(variableName)

    cy.get('textarea').type('1,2,3,4,5,6')

    cy.getByTestID('csv-value-select-dropdown')
      .click()
      .contains('6')
      .click()

      .contains('Create')
      .click()

    cy.getByTestID('search-widget').type(labelName)

    cy.getByTestID('resource-card variable')
      .should('have.length', 1)
      .contains(defaultVar)

    // Delete the label
    cy.getByTestID(`label--pill--delete ${labelName}`).click({force: true})
    cy.getByTestID('resource-card variable').should('have.length', 0)
    cy.getByTestID('search-widget').clear()
    cy.getByTestID('inline-labels--empty').should('exist')

    cy.getByTestID('resource-card variable')
      .should('have.length', 2)
      .first()
      .contains(variableName)
    cy.getByTestID('resource-card variable')
      .last()
      .contains(defaultVar)

    cy.getByTestID('resource-sorter--button').click()
    cy.getByTestID('resource-sorter--name-desc').click()

    cy.getByTestID('resource-card variable')
      .first()
      .contains(defaultVar)
    cy.getByTestID('resource-card variable')
      .last()
      .contains(variableName)
  })
})
