import {Organization, AppState, Dashboard} from '../../src/types'
import {lines} from '../support/commands'

describe('Dashboard', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org).as('org')
    })

    cy.fixture('routes').then(({orgs}) => {
      cy.get('@org').then(({id: orgID}: Organization) => {
        cy.visit(`${orgs}/${orgID}/dashboards-list`)
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
        cy.visit(`${orgs}/${orgID}/dashboards-list`)
      })
    })

    cy.getByTestID('dashboard-card').should('contain', newName)
  })

  it('can create and destroy cells & toggle in and out of presentation mode', () => {
    cy.get('@org').then(({id: orgID}: Organization) => {
      cy.createDashboard(orgID).then(({body}) => {
        cy.fixture('routes').then(({orgs}) => {
          cy.visit(`${orgs}/${orgID}/dashboards/${body.id}`)
        })
      })
    })

    // Create View cell
    cy.getByTestID('add-cell--button').click()
    cy.getByTestID('save-cell--button').click()
    cy.getByTestID('cell-context--toggle').click()
    cy.getByTestID('cell-context--configure').click()

    // Rename View cell
    const xyCellName = 'Line Graph'
    cy.getByTestID('overlay').within(() => {
      cy.getByTestID('page-title').click()
      cy.getByTestID('renamable-page-title--input')
        .clear()
        .type(xyCellName)
      cy.getByTestID('save-cell--button').click()
    })

    const xyCell = `cell ${xyCellName}`

    cy.getByTestID(xyCell).within(([$cell]) => {
      const prevWidth = $cell.clientWidth
      const prevHeight = $cell.clientHeight
      cy.wrap(prevWidth).as('prevWidth')
      cy.wrap(prevHeight).as('prevHeight')
    })

    // Resize Cell
    cy.getByTestID(xyCell).within(() => {
      cy.get('.react-resizable-handle')
        .trigger('mousedown', {which: 1, force: true})
        .trigger('mousemove', {
          clientX: 800,
          clientY: 800,
          force: true,
        })
        .trigger('mouseup', {force: true})
    })

    cy.getByTestID(xyCell).within(([$cell]) => {
      const currWidth = $cell.clientWidth
      const currHeight = $cell.clientHeight
      cy.get('@prevWidth').should('be.lessThan', currWidth)
      cy.get('@prevHeight').should('be.lessThan', currHeight)
    })

    // Note cell
    const noteText = 'this is a note cell'
    const headerPrefix = '#'

    cy.getByTestID('add-note--button').click()
    cy.getByTestID('note-editor--overlay').within(() => {
      cy.get('.CodeMirror').type(`${headerPrefix} ${noteText}`)
      cy.getByTestID('note-editor--preview').contains(noteText)
      cy.getByTestID('note-editor--preview').should('not.contain', headerPrefix)

      cy.getByTestID('save-note--button').click()
    })

    //Note Cell controls
    cy.getByTestID('add-note--button').click()
    cy.getByTestID('note-editor--overlay').should('be.visible')
    cy.getByTestID('cancel-note--button').click()
    cy.getByTestID('note-editor--overlay').should('not.exist')

    const noteCell = 'cell--view-empty markdown'
    cy.getByTestID(noteCell).contains(noteText)
    cy.getByTestID(noteCell).should('not.contain', headerPrefix)

    // Drag and Drop Cell
    cy.getByTestID('cell--draggable Note')
      .trigger('mousedown', {which: 1, force: true})
      .trigger('mousemove', {clientX: -800, clientY: -800, force: true})
      .trigger('mouseup', {force: true})

    cy.getByTestID(noteCell).within(([$cell]) => {
      const noteTop = $cell.getBoundingClientRect().top
      const noteBottom = $cell.getBoundingClientRect().bottom
      cy.wrap(noteTop).as('noteTop')
      cy.wrap(noteBottom).as('noteBottom')
    })

    cy.getByTestID(xyCell).within(([$cell]) => {
      const xyCellTop = $cell.getBoundingClientRect().top
      const xyCellBottom = $cell.getBoundingClientRect().bottom
      cy.get('@noteTop').should('be.lessThan', xyCellTop)
      cy.get('@noteBottom').should('be.lessThan', xyCellBottom)
    })

    // toggle presentation mode
    cy.getByTestID('presentation-mode-toggle').click()
    // ensure a notification is sent when toggling to presentation mode
    cy.getByTestID('notification-primary--children').should('exist')
    // escape to toggle the presentation mode off
    cy.get('body').trigger('keyup', {
      keyCode: 27,
      code: 'Escape',
      key: 'Escape',
    })

    // Remove Note cell
    cy.getByTestID('cell-context--toggle')
      .first()
      .click()
    cy.getByTestID('cell-context--delete').click()
    cy.getByTestID('cell-context--delete-confirm').click()

    // Remove View cell
    cy.getByTestID('cell-context--toggle').click()
    cy.getByTestID('cell-context--delete').click()
    cy.getByTestID('cell-context--delete-confirm').click()

    cy.getByTestID('empty-state').should('exist')
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

    // Add an empty cell
    cy.getByTestID('add-cell--button').click()
    cy.getByTestID('save-cell--button').click()
    cy.getByTestIDSubStr('cell--view-empty').should('be.visible')

    cy.getByTestIDSubStr('cell--view-empty')
      .invoke('text')
      .then(cellContent => {
        // cellContent is yielded as a cutesy phrase from src/shared/copy/cell

        // open Cell Editor Overlay
        cy.getByTestID('cell-context--toggle').click()
        cy.getByTestID('cell-context--configure').click()

        // Cancel edit
        cy.getByTestID('cancel-cell-edit--button').click()

        // Cell content should remain
        cy.getByTestIDSubStr('cell--view-empty').contains(cellContent)
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

  describe('variable interractions', () => {
    beforeEach(() => {
      const numLines = 360
      cy.writeData(lines(numLines))
      cy.get('@org').then(({id: orgID}: Organization) => {
        cy.createDashboard(orgID).then(({body: dashboard}) => {
          cy.wrap({dashboard}).as('dashboard')
        })
      })
    })

    it('can manage variable state with a lot of pointing and clicking', () => {
      const bucketOne = 'b1'
      const bucketThree = 'b3'
      let defaultBucket = ''
      cy.get('@org').then(({id: orgID}: Organization) => {
        cy.get<Dashboard>('@dashboard').then(({dashboard}) => {
          cy.fixture('user').then(({bucket}) => {
            defaultBucket = bucket
            cy.createCSVVariable(orgID, 'bucketsCSV', [
              bucketOne,
              defaultBucket,
              bucketThree,
            ])
          })

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
              .within(() => {
                cy.get('.cf-button').click()
              })
            cy.getByTestID('save-cell--button').click()

            // TESTING CSV VARIABLE
            // selected value in dashboard is 1st value
            cy.getByTestID('variable-dropdown')
              .eq(0)
              .should('contain', bucketOne)
            cy.window()
              .pipe(getSelectedVariable(dashboard.id, 0))
              .should('equal', bucketOne)

            //testing variable controls
            cy.getByTestID('variable-dropdown')
              .eq(0)
              .should('contain', bucketOne)
            cy.getByTestID('variables--button').click()
            cy.getByTestID('variable-dropdown').should('not.exist')
            cy.getByTestID('variables--button').click()
            cy.getByTestID('variable-dropdown').should('exist')

            // sanity check on the url before beginning
            cy.location('search').should('eq', '?lower=now%28%29%20-%201h')

            // select 3rd value in dashboard
            cy.getByTestID('variable-dropdown--button')
              .eq(0)
              .click()
            cy.get(`#${bucketThree}`).click()

            // selected value in dashboard is 3rd value
            cy.getByTestID('variable-dropdown')
              .eq(0)
              .should('contain', bucketThree)
            cy.window()
              .pipe(getSelectedVariable(dashboard.id, 0))
              .should('equal', bucketThree)

            // and that it updates the variable in the URL
            cy.location('search').should(
              'eq',
              `?lower=now%28%29%20-%201h&vars%5BbucketsCSV%5D=${bucketThree}`
            )

            // select 2nd value in dashboard
            cy.getByTestID('variable-dropdown--button')
              .eq(0)
              .click()
            cy.get(`#${defaultBucket}`).click()

            // and that it updates the variable in the URL without breaking stuff
            cy.location('search').should(
              'eq',
              `?lower=now%28%29%20-%201h&vars%5BbucketsCSV%5D=${defaultBucket}`
            )

            // open CEO
            cy.getByTestID('cell-context--toggle').click()
            cy.getByTestID('cell-context--configure').click()

            // selected value in cell context is 2nd value
            cy.window()
              .pipe(getSelectedVariable(dashboard.id, 0))
              .should('equal', defaultBucket)

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
              .should('equal', bucketOne)

            // selected value in dashboard is 1st value
            cy.getByTestID('variable-dropdown').should('contain', bucketOne)
            cy.window()
              .pipe(getSelectedVariable(dashboard.id, 0))
              .should('equal', bucketOne)

            // TESTING MAP VARIABLE
            // selected value in dashboard is 1st value
            cy.getByTestID('variable-dropdown')
              .eq(1)
              .should('contain', 'k1')
            cy.window()
              .pipe(getSelectedVariable(dashboard.id, 2))
              .should('equal', 'v1')

            // select 2nd value in dashboard
            cy.getByTestID('variable-dropdown--button')
              .eq(1)
              .click()
            cy.get(`#k2`).click()

            // selected value in dashboard is 2nd value
            cy.getByTestID('variable-dropdown')
              .eq(1)
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

            cy.getByTestID('cell-context--toggle').click()
            cy.getByTestID('cell-context--delete').click()
            cy.getByTestID('cell-context--delete-confirm').click()

            // create a new cell
            cy.getByTestID('add-cell--button').click()
            cy.getByTestID('switch-to-script-editor').should('be.visible')
            cy.getByTestID('switch-to-script-editor').click()

            // query for data
            cy.getByTestID('flux-editor')
              .should('be.visible')
              .click()
              .focused()
              .clear()
              .type(
                `from(bucket: v.bucketsCSV)
|> range(start: v.timeRangeStart, stop: v.timeRangeStop)
|> filter(fn: (r) => r["_measurement"] == "m")
|> filter(fn: (r) => r["_field"] == "v")
|> filter(fn: (r) => r["tk1"] == "tv1")
|> aggregateWindow(every: v.windowPeriod, fn: max)
|> yield(name: "max")`,
                {force: true, delay: 1}
              )

            // `bucketOne` should not exist nor have data written to it
            cy.getByTestID('save-cell--button').click()
            cy.getByTestID('empty-graph-error').contains(`${bucketOne}`)

            // select bucket "defbuck" that has data
            cy.getByTestID('variable-dropdown--button')
              .eq(0)
              .click()
            cy.get(`#${defaultBucket}`).click()

            // assert visualization appears
            cy.getByTestID('giraffe-layer-line').should('exist')
          })
        })
      })
    })

    it('ensures that dependent variables load one another accordingly', () => {
      cy.get('@org').then(({id: orgID}: Organization) => {
        cy.createDashboard(orgID).then(({body: dashboard}) => {
          const now = Date.now()
          cy.writeData([
            `test,container_name=cool dopeness=12 ${now - 1000}000000`,
            `test,container_name=beans dopeness=18 ${now - 1200}000000`,
            `test,container_name=cool dopeness=14 ${now - 1400}000000`,
            `test,container_name=beans dopeness=10 ${now - 1600}000000`,
          ])
          cy.createCSVVariable(orgID, 'static', ['beans', 'defbuck'])
          cy.createQueryVariable(
            orgID,
            'dependent',
            `import "influxdata/influxdb/v1"
            v1.tagValues(bucket: v.static, tag: "container_name") |> keep(columns: ["_value"])`
          )
          cy.createQueryVariable(
            orgID,
            'build',
            `import "influxdata/influxdb/v1"
            import "strings"
            v1.tagValues(bucket: v.static, tag: "container_name") |> filter(fn: (r) => strings.hasSuffix(v: r._value, suffix: v.dependent))`
          )

          cy.fixture('routes').then(({orgs}) => {
            cy.visit(`${orgs}/${orgID}/dashboards/${dashboard.id}`)
          })
        })

        cy.getByTestID('add-cell--button').click()
        cy.getByTestID('switch-to-script-editor').should('be.visible')
        cy.getByTestID('switch-to-script-editor').click()
        cy.getByTestID('toolbar-tab').click()

        cy
          .getByTestID('flux-editor')
          .should('be.visible')
          .click()
          .focused().type(`from(bucket: v.static)
|> range(start: v.timeRangeStart, stop: v.timeRangeStop)
|> filter(fn: (r) => r["_measurement"] == "test")
|> filter(fn: (r) => r["_field"] == "dopeness")
|> filter(fn: (r) => r["container_name"] == v.build)`)

        cy.getByTestID('save-cell--button').click()

        // the default bucket selection should have no results and load all three variables
        // even though only two variables are being used (because 1 is dependent upon another)
        cy.getByTestID('variable-dropdown')
          .should('have.length', 3)
          .eq(0)
          .should('contain', 'beans')

        // and cause the rest to exist in loading states
        cy.getByTestID('variable-dropdown')
          .eq(1)
          .should('contain', 'Loading')

        cy.getByTestIDSubStr('cell--view-empty')

        // But selecting a nonempty bucket should load some data
        cy.getByTestID('variable-dropdown--button')
          .eq(0)
          .click()
        cy.get(`#defbuck`).click()

        // default select the first result
        cy.getByTestID('variable-dropdown')
          .eq(1)
          .should('contain', 'beans')

        // and also load the third result
        cy.getByTestID('variable-dropdown--button')
          .eq(2)
          .should('contain', 'beans')
          .click()
        cy.get(`#cool`).click()

        // and also load the second result
        cy.getByTestID('variable-dropdown')
          .eq(1)
          .should('contain', 'cool')

        // updating the third variable should update the second
        cy.getByTestID('variable-dropdown--button')
          .eq(2)
          .click()
        cy.get(`#beans`).click()
        cy.getByTestID('variable-dropdown')
          .eq(1)
          .should('contain', 'beans')
      })
    })

    /*\
    built to approximate an instance with docker metrics,
    operating with the variables:
        depbuck:
            from(bucket: v.buckets)
                |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
                |> filter(fn: (r) => r["_measurement"] == "docker_container_cpu")
                |> keep(columns: ["container_name"])
                |> rename(columns: {"container_name": "_value"})
                |> last()
                |> group()
        buckets:
            buckets()
                |> filter(fn: (r) => r.name !~ /^_/)
                |> rename(columns: {name: "_value"})
                |> keep(columns: ["_value"])
    and a dashboard built of :
        cell one:
            from(bucket: v.buckets)
                |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
                |> filter(fn: (r) => r["_measurement"] == "docker_container_cpu")
                |> filter(fn: (r) => r["_field"] == "usage_percent")
        cell two:
            from(bucket: v.buckets)
                |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
                |> filter(fn: (r) => r["_measurement"] == "docker_container_cpu")
                |> filter(fn: (r) => r["_field"] == "usage_percent")
                |> filter(fn: (r) => r["container_name"] == v.depbuck)
    with only 4 api queries being sent to fulfill it all
  \*/
    it('can load dependent queries without much fuss', () => {
      cy.get('@org').then(({id: orgID}: Organization) => {
        cy.createDashboard(orgID).then(({body: dashboard}) => {
          const now = Date.now()
          cy.writeData([
            `test,container_name=cool dopeness=12 ${now - 1000}000000`,
            `test,container_name=beans dopeness=18 ${now - 1200}000000`,
            `test,container_name=cool dopeness=14 ${now - 1400}000000`,
            `test,container_name=beans dopeness=10 ${now - 1600}000000`,
          ])
          cy.createCSVVariable(orgID, 'static', ['beans', 'defbuck'])
          cy.createQueryVariable(
            orgID,
            'dependent',
            `from(bucket: v.static)
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "test")
  |> keep(columns: ["container_name"])
  |> rename(columns: {"container_name": "_value"})
  |> last()
  |> group()`
          )

          cy.fixture('routes').then(({orgs}) => {
            cy.visit(`${orgs}/${orgID}/dashboards/${dashboard.id}`)
          })
        })
      })

      cy.getByTestID('add-cell--button').click()
      cy.getByTestID('switch-to-script-editor').should('be.visible')
      cy.getByTestID('switch-to-script-editor').click()
      cy.getByTestID('toolbar-tab').click()

      cy
        .getByTestID('flux-editor')
        .should('be.visible')
        .click()
        .focused().type(`from(bucket: v.static)
|> range(start: v.timeRangeStart, stop: v.timeRangeStop)
|> filter(fn: (r) => r["_measurement"] == "test")
|> filter(fn: (r) => r["_field"] == "dopeness")
|> filter(fn: (r) => r["container_name"] == v.dependent)`)
      cy.getByTestID('save-cell--button').click()

      // the default bucket selection should have no results
      cy.getByTestID('variable-dropdown')
        .eq(0)
        .should('contain', 'beans')

      // and cause the rest to exist in loading states
      cy.getByTestID('variable-dropdown')
        .eq(1)
        .should('contain', 'Loading')

      cy.getByTestIDSubStr('cell--view-empty')

      // But selecting a nonempty bucket should load some data
      cy.getByTestID('variable-dropdown--button')
        .eq(0)
        .click()
      cy.get(`#defbuck`).click()

      // default select the first result
      cy.getByTestID('variable-dropdown')
        .eq(1)
        .should('contain', 'beans')

      // and also load the second result
      cy.getByTestID('variable-dropdown--button')
        .eq(1)
        .click()
      cy.get(`#cool`).click()
    })
  })

  it('can create a view through the API', () => {
    cy.get('@org').then(({id: orgID}: Organization) => {
      cy.createDashWithViewAndVar(orgID).then(() => {
        cy.fixture('routes').then(({orgs}) => {
          cy.visit(`${orgs}/${orgID}/dashboards-list`)
          cy.getByTestID('dashboard-card--name').click()
          cy.get('.cell--view').should('have.length', 1)
        })
      })
    })
  })

  it("should return empty table parameters when query hasn't been submitted", () => {
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
