import {Organization, AppState} from '../../src/types'

describe('Dashboard', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org).as('org')
    })

    cy.fixture('routes').then(({orgs}) => {
      cy.get('@org').then(({id: orgID}: Organization) => {
        cy.visit(`${orgs}/${orgID}/dashboards`)
      })
    })
  })

  it("can edit a dashboard's name", () => {
    cy.get('@org').then(({id: orgID}: Organization) => {
      cy.createDashboard(orgID).then(({body}) => {
        cy.fixture('routes').then(({orgs}) => {
          cy.visit(`${orgs}/${orgID}/dashboards/${body.id}`)
        })
      })
    })

    const newName = 'new 🅱️ashboard'

    cy.get('.renamable-page-title').click()
    cy.get('.cf-input-field')
      .type(newName)
      .type('{enter}')

    cy.fixture('routes').then(({orgs}) => {
      cy.get('@org').then(({id: orgID}: Organization) => {
        cy.visit(`${orgs}/${orgID}/dashboards`)
      })
    })

    cy.getByTestID('dashboard-card').should('contain', newName)
  })

  it('can create a cell', () => {
    cy.get('@org').then(({id: orgID}: Organization) => {
      cy.createDashboard(orgID).then(({body}) => {
        cy.fixture('routes').then(({orgs}) => {
          cy.visit(`${orgs}/${orgID}/dashboards/${body.id}`)
        })
      })
    })

    cy.getByTestID('add-cell--button').click()
    cy.getByTestID('save-cell--button').click()
    cy.getByTestID('cell--view-empty').should('have.length', 1)
  })

  // fix for https://github.com/influxdata/influxdb/issues/15239
  it('retains the cell content after canceling an edit to the cell', () => {
    cy.get('@org').then(({id: orgID}: Organization) => {
      cy.createDashboard(orgID).then(({body}) => {
        cy.fixture('routes').then(({orgs}) => {
          cy.visit(`${orgs}/${orgID}/dashboards/${body.id}`)
        })
      })
    })

    // Add an empty celly cell
    cy.getByTestID('add-cell--button').click()
    cy.getByTestID('save-cell--button').click()
    cy.getByTestID('cell--view-empty').should('be.visible')

    cy.getByTestID('cell--view-empty')
      .invoke('text')
      .then(cellContent => {
        // cellContent is yielded as a cutesy phrase from src/shared/copy/cell

        // open Cell Editor Overlay
        cy.getByTestID('cell-context--toggle').click()
        cy.getByTestID('cell-context--configure').click()

        // Cancel edit
        cy.getByTestID('cancel-cell-edit--button').click()

        // Cell content should remain
        cy.getByTestID('cell--view-empty').contains(cellContent)
      })
  })

  const getSelectedVariable = (contextID: string, index?: number) => win => {
    const state = win.store.getState() as AppState
    const defaultVarOrder = state.resources.variables.allIDs
    const defaultVarDawg =
      state.resources.variables.byID[defaultVarOrder[index]] || {}
    const filledVarDawg =
      (state.resources.variables.values[contextID] || {values: {}}).values[
        defaultVarOrder[index]
      ] || {}

    const hydratedVarDawg = {
      ...defaultVarDawg,
      ...filledVarDawg,
    }

    if (hydratedVarDawg.arguments.type === 'map') {
      if (!hydratedVarDawg.selected) {
        hydratedVarDawg.selected = [
          Object.keys(hydratedVarDawg.arguments.values)[0],
        ]
      }

      return hydratedVarDawg.arguments.values[hydratedVarDawg.selected[0]]
    }

    if (!hydratedVarDawg.selected) {
      hydratedVarDawg.selected = [hydratedVarDawg.arguments.values[0]]
    }

    return hydratedVarDawg.selected[0]
  }

  it('can manage variable state with a lot of pointing and clicking', () => {
    cy.get('@org').then(({id: orgID}: Organization) => {
      cy.createDashboard(orgID).then(({body: dashboard}) => {
        cy.createCSVVariable(orgID)
        cy.createQueryVariable(orgID)
        cy.createMapVariable(orgID).then(() => {
          cy.fixture('routes').then(({orgs}) => {
            cy.visit(`${orgs}/${orgID}/dashboards/${dashboard.id}`)
          })
          // add cell with variable in its query
          cy.getByTestID('add-cell--button').click()
          cy.getByTestID('switch-to-script-editor').should('be.visible')
          cy.getByTestID('switch-to-script-editor').click()
          cy.getByTestID('toolbar-tab').click()
          // check to see if the default timeRange variables are available
          cy.get('.flux-toolbar--list-item').contains('timeRangeStart')
          cy.get('.flux-toolbar--list-item').contains('timeRangeStop')
          cy.get('.flux-toolbar--list-item')
            .first()
            .within(() => {
              cy.get('.cf-button').click()
            })

          cy.getByTestID('flux-editor')
            .should('be.visible')
            .click()
            .focused()
            .type(' ')
          cy.get('.flux-toolbar--list-item')
            .eq(2)
            .click()
          cy.getByTestID('save-cell--button').click()

          // TESTING CSV VARIABLE
          // selected value in dashboard is 1st value
          cy.getByTestID('variable-dropdown')
            .eq(0)
            .should('contain', 'c1')
          cy.window()
            .pipe(getSelectedVariable(dashboard.id, 0))
            .should('equal', 'c1')

          // select 2nd value in dashboard
          cy.getByTestID('variable-dropdown--button')
            .eq(0)
            .click()
          cy.get(`#c2`).click()
          // breaks here

          // selected value in dashboard is 2nd value
          cy.getByTestID('variable-dropdown')
            .eq(0)
            .should('contain', 'c2')
          cy.window()
            .pipe(getSelectedVariable(dashboard.id, 0))
            .should('equal', 'c2')

          // open CEO
          cy.getByTestID('cell-context--toggle').click()
          cy.getByTestID('cell-context--configure').click()

          // selected value in cell context is 2nd value
          cy.window()
            .pipe(getSelectedVariable(dashboard.id, 0))
            .should('equal', 'c2')

          cy.getByTestID('toolbar-tab').click()
          cy.get('.flux-toolbar--list-item')
            .first()
            .trigger('mouseover')
          // toggle the variable dropdown in the VEO cell dashboard
          cy.getByTestID('toolbar-popover--contents').within(() => {
            cy.getByTestID('variable-dropdown--button').click()
            // select 1st value in cell
            cy.getByTestID('variable-dropdown--item')
              .first()
              .click()
          })
          // save cell
          cy.getByTestID('save-cell--button').click()

          // selected value in cell context is 1st value
          cy.window()
            .pipe(getSelectedVariable(dashboard.id, 0))
            .should('equal', 'c1')

          // selected value in dashboard is 1st value
          cy.getByTestID('variable-dropdown').should('contain', 'c1')
          cy.window()
            .pipe(getSelectedVariable(dashboard.id, 0))
            .should('equal', 'c1')

          // TESTING MAP VARIABLE
          // selected value in dashboard is 1st value
          cy.getByTestID('variable-dropdown')
            .eq(2)
            .should('contain', 'k1')
          cy.window()
            .pipe(getSelectedVariable(dashboard.id, 2))
            .should('equal', 'v1')

          // select 2nd value in dashboard
          cy.getByTestID('variable-dropdown--button')
            .eq(2)
            .click()
          cy.get(`#k2`).click()

          // selected value in dashboard is 2nd value
          cy.getByTestID('variable-dropdown')
            .eq(2)
            .should('contain', 'k2')
          cy.window()
            .pipe(getSelectedVariable(dashboard.id, 2))
            .should('equal', 'v2')

          // open CEO
          cy.getByTestID('cell-context--toggle').click()
          cy.getByTestID('cell-context--configure').click()
          cy.getByTestID('toolbar-tab').should('be.visible')

          // selected value in cell context is 2nd value
          cy.window()
            .pipe(getSelectedVariable(dashboard.id, 2))
            .should('equal', 'v2')

          cy.getByTestID('toolbar-tab').click()
          cy.get('.flux-toolbar--list-item')
            .eq(2)
            .trigger('mouseover')
          // toggle the variable dropdown in the VEO cell dashboard
          cy.getByTestID('toolbar-popover--contents').within(() => {
            cy.getByTestID('variable-dropdown--button').click()
            // select 1st value in cell
            cy.getByTestID('variable-dropdown--item')
              .first()
              .click()
          })
          // save cell
          cy.getByTestID('save-cell--button').click()

          // selected value in cell context is 1st value
          cy.window()
            .pipe(getSelectedVariable(dashboard.id, 2))
            .should('equal', 'v1')

          // selected value in dashboard is 1st value
          cy.getByTestID('variable-dropdown').should('contain', 'k1')
          cy.window()
            .pipe(getSelectedVariable(dashboard.id, 2))
            .should('equal', 'v1')
        })
      })
    })
  })

  it('can create a view through the API', () => {
    cy.get('@org').then(({id: orgID}: Organization) => {
      cy.createDashWithViewAndVar(orgID).then(() => {
        cy.fixture('routes').then(({orgs}) => {
          cy.visit(`${orgs}/${orgID}/dashboards`)
          cy.getByTestID('dashboard-card--name').click()
          cy.get('.cell--view').should('have.length', 1)
        })
      })
    })
  })

  it("Should return empty table parameters when query hasn't been submitted", () => {
    cy.get('@org').then(({id: orgID}: Organization) => {
      cy.createDashboard(orgID).then(({body}) => {
        cy.fixture('routes').then(({orgs}) => {
          cy.visit(`${orgs}/${orgID}/dashboards/${body.id}`)
        })
      })
    })

    cy.getByTestID('add-cell--button')
      .click()
      .then(() => {
        cy.get('.view-options').should('not.exist')
        cy.getByTestID('cog-cell--button')
          .should('have.length', 1)
          .click()
        // should toggle the view options
        cy.get('.view-options').should('exist')
        cy.getByTestID('dropdown--button')
          .contains('Graph')
          .click()
          .then(() => {
            cy.getByTestID('view-type--table')
              .contains('Table')
              .should('have.length', 1)
              .click()

            cy.getByTestID('empty-state--text')
              .contains('This query returned no columns')
              .should('exist')
          })
      })
  })
})
