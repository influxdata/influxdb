import {Organization} from '../../src/types'
import {VIS_TYPES} from '../../src/timeMachine/constants'
import {
  FROM,
  RANGE,
  MEAN,
  MATH_ABS,
  MATH_FLOOR,
  STRINGS_TITLE,
  STRINGS_TRIM,
} from '../../src/shared/constants/fluxFunctions'

const TYPE_DELAY = 0

function getTimeMachineText() {
  return cy
    .wrap({
      text: () => {
        const store = cy.state().window.store.getState().timeMachines
        const timeMachine = store.timeMachines[store.activeTimeMachineID]
        const query =
          timeMachine.draftQueries[timeMachine.activeQueryIndex].text
        return query
      },
    })
    .invoke('text')
}

describe('DataExplorer', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org).as('org')
      cy.createMapVariable(body.org.id)
      cy.wrap(body.bucket).as('bucket')
    })

    cy.fixture('routes').then(({orgs, explorer}) => {
      cy.get<Organization>('@org').then(({id}) => {
        cy.visit(`${orgs}/${id}${explorer}`)
      })
    })
  })

  describe('numeric input using custom bin sizes in Histograms', () => {
    beforeEach(() => {
      cy.getByTestID('view-type--dropdown').click()
      cy.getByTestID(`view-type--histogram`).click()
      cy.getByTestID('cog-cell--button').click()
    })

    it('should put input field in error status and stay in error status when input is invalid or empty', () => {
      cy.get('.view-options').within(() => {
        cy.getByTestID('auto-input').within(() => {
          cy.getByTestID('input-field')
            .click()
            .type('{backspace}{backspace}')
          cy.getByTestID('auto-input--custom').should(
            'have.class',
            'cf-select-group--option__active'
          )
          cy.getByTestID('input-field--error').should('have.length', 1)
          cy.getByTestID('input-field').type('adfuiopbvmc')
          cy.getByTestID('input-field--error').should('have.length', 1)
        })
      })
    })

    it('should not have the input field in error status when input becomes valid', () => {
      cy.get('.view-options').within(() => {
        cy.getByTestID('auto-input').within(() => {
          cy.getByTestID('input-field')
            .click()
            .type('{backspace}{backspace}3')
          cy.getByTestID('input-field--error').should('have.length', 0)
        })
      })
    })
  })

  describe('numeric input validation when changing bin sizes in Heat Maps', () => {
    beforeEach(() => {
      cy.getByTestID('view-type--dropdown').click()
      cy.getByTestID(`view-type--heatmap`).click()
      cy.getByTestID('cog-cell--button').click()
    })

    it('should put input field in error status and stay in error status when input is invalid or empty', () => {
      cy.get('.view-options').within(() => {
        cy.getByTestID('grid--column').within(() => {
          cy.getByTestID('input-field')
            .first()
            .click()
            .type('{backspace}')
            .getByTestID('input-field--error')
            .should('have.length', 1)
          cy.getByTestID('input-field')
            .first()
            .click()
            .type('{backspace}')
            .getByTestID('input-field--error')
            .should('have.length', 1)
          cy.getByTestID('input-field')
            .first()
            .click()
            .type('4')
            .getByTestID('input-field--error')
            .should('have.length', 1)
          cy.getByTestID('input-field')
            .first()
            .click()
            .type('{backspace}abcdefg')
            .getByTestID('input-field--error')
            .should('have.length', 1)
        })
      })
    })

    it('should not have input field in error status when "10" becomes valid input such as "5"', () => {
      cy.get('.view-options').within(() => {
        cy.getByTestID('grid--column').within(() => {
          cy.getByTestID('input-field')
            .first()
            .click()
            .type('{backspace}{backspace}5')
            .getByTestID('input-field--error')
            .should('have.length', 0)
        })
      })
    })

    it('should error when value is above "670" in bin size', () => {
      cy.get('.view-options').within(() => {
        cy.getByTestID('grid--column').within(() => {
          cy.getByTestID('bin-size-input')
            .clear()
            .type('670')
            .getByTestID('bin-size-input--error')
            .should('have.length', 1)
        })
      })
    })
  })

  describe('numeric input validation when changing number of decimal places in Single Stat', () => {
    beforeEach(() => {
      cy.getByTestID('view-type--dropdown').click()
      cy.getByTestID(`view-type--single-stat`).click()
      cy.getByTestID('cog-cell--button').click()
    })

    it('should put input field in error status and stay in error status when input is invalid or empty', () => {
      cy.get('.view-options').within(() => {
        cy.getByTestID('auto-input--input').within(() => {
          cy.getByTestID('input-field')
            .click()
            .type('{backspace}')
            .invoke('attr', 'type')
            .should('equal', 'text')
            .getByTestID('input-field--error')
            .should('have.length', 1)
          cy.getByTestID('input-field')
            .click()
            .type('{backspace}')
            .invoke('val')
            .should('equal', '')
            .getByTestID('input-field--error')
            .should('have.length', 1)
          cy.getByTestID('input-field')
            .click()
            .type('abcdefg')
            .invoke('val')
            .should('equal', '')
            .getByTestID('input-field--error')
            .should('have.length', 1)
        })
      })
    })

    it('should not have input field in error status when "2" becomes valid input such as "11"', () => {
      cy.get('.view-options').within(() => {
        cy.getByTestID('auto-input--input').within(() => {
          cy.getByTestID('input-field')
            .click()
            .type('{backspace}11')
            .invoke('val')
            .should('equal', '11')
            .getByTestID('input-field--error')
            .should('have.length', 0)
        })
      })
    })
  })

  describe('Optional suffix and prefix in gauge', () => {
    beforeEach(() => {
      cy.getByTestID('view-type--dropdown').click()
      cy.getByTestID(`view-type--gauge`).click()
      cy.getByTestID('cog-cell--button').click()
    })
    it('can add prefix and suffix values', () => {
      cy.get('.view-options').within(() => {
        cy.getByTestID('prefix-input')
          .click()
          .type('mph')
          .invoke('val')
          .should('equal', 'mph')
          .getByTestID('input-field--error')
          .should('have.length', 0)
        cy.getByTestID('suffix-input')
          .click()
          .type('mph')
          .invoke('val')
          .should('equal', 'mph')
          .getByTestID('input-field--error')
          .should('have.length', 0)
      })
    })
    it('can add and remove tick values', () => {
      cy.get('.view-options').within(() => {
        cy.getByTestID('tickprefix-input')
          .click()
          .invoke('val')
          .should('equal', '')
          .getByTestID('input-field--error')
          .should('have.length', 0)
        cy.getByTestID('ticksuffix-input')
          .click()
          .invoke('val')
          .should('equal', '')
          .getByTestID('input-field--error')
          .should('have.length', 0)
      })
    })
  })

  describe('select time range to query', () => {
    it('can select different time ranges', () => {
      // find initial value
      cy.get('.cf-dropdown--selected')
        .contains('Past 1')
        .should('have.length', 1)
      cy.getByTestID('timerange-popover--dialog').should('not.exist')
      cy.getByTestID('timerange-dropdown').click()

      cy.getByTestID('dropdown-item-past15m').click()
      cy.get('.cf-dropdown--selected')
        .contains('Past 15m')
        .should('have.length', 1)

      cy.getByTestID('timerange-dropdown').click()

      cy.getByTestID('timerange-popover--dialog').should('not.exist')

      cy.getByTestID('dropdown-item-customtimerange').click()
      cy.getByTestID('timerange-popover--dialog').should('have.length', 1)
    })

    describe('should allow for custom time range selection', () => {
      beforeEach(() => {
        cy.getByTestID('timerange-dropdown').click()
        cy.getByTestID('dropdown-item-customtimerange').click()
        cy.getByTestID('timerange-popover--dialog').should('have.length', 1)
      })

      it.skip('should error when submitting stop dates that are before start dates', () => {
        // TODO: complete with issue #15632
        // https://github.com/influxdata/influxdb/issues/15632
        cy.get('input[title="Start"]')
          .should('have.length', 1)
          .clear()
          .type('2019-10-31')

        cy.get('input[title="Stop"]')
          .should('have.length', 1)
          .clear()
          .type('2019-10-29')

        // click button and see if time range has been selected
        cy.get('.cf-button--label')
          .contains('Apply Time Range')
          .should('have.length', 1)
          .click()

        // TODO: complete test once functionality is fleshed out

        // cy.get('.cf-dropdown--selected')
        //   .contains('2019-10-01 00:00 - 2019-10-31 00:00')
        //   .should('have.length', 1)
      })

      it.skip('should error when invalid dates are input', () => {
        // TODO: complete with issue #15632
        // https://github.com/influxdata/influxdb/issues/15632
        // default inputs should be valid
        cy.getByTestID('input-error').should('not.exist')

        // type incomplete input
        cy.get('input[title="Start"]')
          .should('have.length', 1)
          .clear()
          .type('2019-10')

        // invalid date errors
        cy.getByTestID('input-error').should('have.length', 1)

        // modifies the input to be valid
        cy.get('input[title="Start"]').type('-01')

        // warnings should not appear
        cy.getByTestID('input-error').should('not.exist')

        // type invalid stop date
        cy.get('input[title="Stop"]')
          .should('have.length', 1)
          .clear()
          .type('2019-10-')

        // invalid date errors
        cy.getByTestID('input-error').should('have.length', 1)

        // try submitting invalid date
        cy.get('.cf-button--label')
          .contains('Apply Time Range')
          .should('have.length', 1)
          .click()

        // TODO: complete test once functionality is fleshed out

        // cy.get('.cf-dropdown--selected')
        //   .contains('2019-10-01 00:00 - 2019-10-31 00:00')
        //   .should('have.length', 1)
      })

      it('can set a custom time range', () => {
        // set the start and stop dates
        cy.get('input[title="Start"]')
          .should('have.length', 1)
          .clear()
          .type('2019-10-01')

        cy.get('input[title="Stop"]')
          .should('have.length', 1)
          .clear()
          .type('2019-10-31')

        // click button and see if time range has been selected
        cy.get('.cf-button--label')
          .contains('Apply Time Range')
          .should('have.length', 1)
          .click()

        cy.get('.cf-dropdown--selected')
          .contains('2019-10-01 00:00 - 2019-10-31 00:00')
          .should('have.length', 1)
      })
    })
  })

  describe('editing mode switching', () => {
    const measurement = 'my_meas'
    const field = 'my_field'

    beforeEach(() => {
      cy.writeData([`${measurement} ${field}=0`, `${measurement} ${field}=1`])
    })

    it('can switch to and from script editor mode', () => {
      cy.getByTestID('selector-list my_meas').click()
      cy.getByTestID('selector-list my_field').click()

      cy.getByTestID('switch-to-script-editor').click()
      cy.getByTestID('flux-editor').should('exist')

      // revert back to query builder mode (without confirmation)
      cy.getByTestID('switch-to-query-builder').click()
      cy.getByTestID('query-builder').should('exist')

      // can revert back to query builder mode (with confirmation)
      cy.getByTestID('switch-to-script-editor')
        .should('be.visible')
        .click()
      cy.getByTestID('flux--aggregate.rate--inject').click()
      // check to see if import is defaulted to the top
      cy.get('.view-line')
        .first()
        .contains('import')
      // check to see if new aggregate rate is at the bottom
      cy.get('.view-line')
        .last()
        .contains('aggregate.')
      cy.getByTestID('flux-editor').should('exist')
      cy.getByTestID('flux-editor').within(() => {
        cy.get('textarea').type('yoyoyoyoyo', {force: true})
      })

      // can hover over flux functions
      cy.getByTestID('flux-docs--aggregateWindow').should('not.exist')
      cy.getByTestID('flux--aggregateWindow').trigger('mouseover')
      cy.getByTestID('flux-docs--aggregateWindow').should('exist')

      cy.getByTestID('switch-query-builder-confirm--button').click()

      cy.getByTestID('switch-query-builder-confirm--popover--contents').within(
        () => {
          cy.getByTestID('switch-query-builder-confirm--confirm-button').click()
        }
      )

      cy.getByTestID('query-builder').should('exist')
    })
  })

  describe('raw script editing', () => {
    beforeEach(() => {
      cy.getByTestID('switch-to-script-editor')
        .should('be.visible')
        .click()
    })

    it('shows flux errors', () => {
      cy.getByTestID('time-machine--bottom').then(() => {
        cy.getByTestID('flux-editor').within(() => {
          cy.get('textarea').type('foo |> bar', {force: true})

          cy.get('.squiggly-error').should('be.visible')
        })
      })
    })

    it('shows flux signatures', () => {
      cy.getByTestID('time-machine--bottom').then(() => {
        cy.getByTestID('flux-editor').within(() => {
          cy.get('textarea').type('from(', {force: true})

          cy.get('.signature').should('be.visible')
        })
      })
    })

    it('enables the submit button when a query is typed', () => {
      cy.getByTestID('time-machine-submit-button').should('be.disabled')

      cy.getByTestID('flux-editor').within(() => {
        cy.get('.react-monaco-editor-container')
          .should('be.visible')
          .click()
          .focused()
          .type('yo', {force: true, delay: TYPE_DELAY})
        cy.getByTestID('time-machine-submit-button').should('not.be.disabled')
      })
    })

    it('disables submit when a query is deleted', () => {
      cy.getByTestID('time-machine--bottom').then(() => {
        cy.get('.react-monaco-editor-container')
          .should('be.visible')
          .click()
          .focused()
          .type('from(bucket: "foo")', {force: true, delay: TYPE_DELAY})

        cy.getByTestID('time-machine-submit-button').should('not.be.disabled')

        cy.get('.react-monaco-editor-container')
          .should('be.visible')
          .click()
          .focused()
          .type('{selectall} {backspace}', {force: true, delay: TYPE_DELAY})
      })

      cy.getByTestID('time-machine-submit-button').should('be.disabled')
    })

    it('imports the appropriate packages to build a query', () => {
      cy.getByTestID('functions-toolbar-tab').click()

      cy.getByTestID('flux--from--inject').click()
      cy.getByTestID('flux--range--inject').click()
      cy.getByTestID('flux--math.abs--inject').click()
      cy.getByTestID('flux--math.floor--inject').click()
      cy.getByTestID('flux--strings.title--inject').click()
      cy.getByTestID('flux--strings.trim--inject').click()

      cy.wait(100)

      getTimeMachineText().then(text => {
        const expected = `
        import"${STRINGS_TITLE.package}"
        import"${MATH_ABS.package}"
        ${FROM.example}|>
        ${RANGE.example}|>
        ${MATH_ABS.example}|>
        ${MATH_FLOOR.example}|>
        ${STRINGS_TITLE.example}|>
        ${STRINGS_TRIM.example}`

        cy.fluxEqual(text, expected).should('be.true')
      })
    })

    it('can use the function selector to build a query', () => {
      cy.getByTestID('functions-toolbar-tab').click()

      cy.getByTestID('flux--from--inject').click()

      getTimeMachineText().then(text => {
        const expected = FROM.example

        cy.fluxEqual(text, expected).should('be.true')
      })

      cy.getByTestID('flux--range--inject').click()

      getTimeMachineText().then(text => {
        const expected = `${FROM.example}|>${RANGE.example}`

        cy.fluxEqual(text, expected).should('be.true')
      })

      cy.getByTestID('flux--mean--inject').click()

      getTimeMachineText().then(text => {
        const expected = `${FROM.example}|>${RANGE.example}|>${MEAN.example}`

        cy.fluxEqual(text, expected).should('be.true')
      })
    })

    it('can filter aggregation functions by name from script editor mode', () => {
      cy.get('.cf-input-field').type('covariance')
      cy.get('.flux-toolbar--list-item').should('have.length', 1)
    })

    it('shows the empty state when the query returns no results', () => {
      cy.getByTestID('time-machine--bottom').within(() => {
        cy.get('.react-monaco-editor-container')
          .should('be.visible')
          .click()
          .focused()
          .type(
            `from(bucket: "defbuck"{rightarrow}
  |> range(start: -10s{rightarrow}
  |> filter(fn: (r{rightarrow} => r._measurement == "no exist"{rightarrow}`,
            {force: true, delay: TYPE_DELAY}
          )
        cy.getByTestID('time-machine-submit-button').click()
      })

      cy.getByTestID('empty-graph--no-results').should('exist')
    })

    it('can save query as task even when it has a variable', () => {
      const taskName = 'tax'
      // begin flux
      cy.getByTestID('flux-editor').within(() => {
        cy.get('.react-monaco-editor-container')
          .should('be.visible')
          .click()
          .focused()
          .type(
            `from(bucket: "defbuck"{rightarrow}
  |> range(start: -15m, stop: now({rightarrow}{rightarrow}
  |> filter(fn: (r{rightarrow} => r._measurement ==`,
            {force: true, delay: TYPE_DELAY}
          )
      })

      cy.getByTestID('toolbar-tab').click()
      // checks to see if the default variables exist
      cy.getByTestID('variable--timeRangeStart')
      cy.getByTestID('variable--timeRangeStop')
      cy.getByTestID('variable--windowPeriod')
      //insert variable name by clicking on variable
      cy.get('.flux-toolbar--variable')
        .first()
        .within(() => {
          cy.contains('Inject').click()
        })

      cy.getByTestID('save-query-as').click()
      cy.getByTestID('task--radio-button').click()
      cy.getByTestID('task-form-name').type(taskName)
      cy.getByTestID('task-form-schedule-input').type('4h')
      cy.getByTestID('task-form-save').click()

      cy.getByTestID(`task-card`)
        .should('exist')
        .should('contain', taskName)
    })
  })

  describe('query builder', () => {
    it('shows an empty state for tag keys when the bucket is empty', () => {
      cy.getByTestID('empty-tag-keys').should('exist')
    })

    it('shows the correct number of buckets in the buckets dropdown', () => {
      cy.get<Organization>('@org').then(({id, name}) => {
        cy.createBucket(id, name, 'newBucket')
      })

      cy.getByTestID('selector-list defbuck').should('exist')
      cy.getByTestID('selector-list newBucket').should('exist')
    })

    it('can delete a second query', () => {
      cy.get('.time-machine-queries--new').click()
      cy.get('.query-tab').should('have.length', 2)
      cy.get('.query-tab--close')
        .first()
        // Element is only visible on hover, using force to make this test pass
        .click({force: true})
      cy.get('.query-tab').should('have.length', 1)
    })

    it('can remove a second query using tab context menu', () => {
      cy.get('.query-tab').trigger('contextmenu')
      cy.getByTestID('right-click--remove-tab').should(
        'have.class',
        'cf-right-click--menu-item__disabled'
      )

      // Fire a click outside of the right click menu to dismiss it because
      // it is obscuring the + button
      cy.getByTestID('page-header').click()

      cy.get('.time-machine-queries--new').click()
      cy.get('.query-tab').should('have.length', 2)

      cy.get('.query-tab')
        .first()
        .trigger('contextmenu')
      cy.getByTestID('right-click--remove-tab').click()

      cy.get('.query-tab').should('have.length', 1)
    })
  })

  describe('visualizations', () => {
    describe('empty states', () => {
      it('shows a message if no queries have been created', () => {
        cy.getByTestID('empty-graph--no-queries').should('exist')
      })

      it('shows an error if a query is syntactically invalid', () => {
        cy.getByTestID('switch-to-script-editor').click()

        cy.getByTestID('time-machine--bottom').within(() => {
          const remove = cy.state().window.store.subscribe(() => {
            remove()
            cy.getByTestID('time-machine-submit-button').click()
            cy.getByTestID('empty-graph--error').should('exist')
          })
          cy.get('.react-monaco-editor-container')
            .click()
            .focused()
            .type('from(', {force: true, delay: 2})
          cy.getByTestID('time-machine-submit-button').click()
        })
      })
    })

    describe('visualize with 360 lines', () => {
      const numLines = 360
      beforeEach(() => {
        // POST 360 lines to the server
        cy.writeData(lines(numLines))
      })

      it('can view time-series data', () => {
        // build the query to return data from beforeEach
        cy.getByTestID(`selector-list m`).click()
        cy.getByTestID('selector-list v').click()
        cy.getByTestID(`selector-list tv1`).click()
        cy.getByTestID('selector-list max').click()

        cy.getByTestID('time-machine-submit-button').click()

        // cycle through all the visualizations of the data
        VIS_TYPES.forEach(({type}) => {
          cy.getByTestID('view-type--dropdown').click()
          cy.getByTestID(`view-type--${type}`).click()
          cy.getByTestID(`vis-graphic--${type}`).should('exist')
          if (type.includes('single-stat')) {
            cy.getByTestID('single-stat--text').should('contain', `${numLines}`)
          }
        })

        // view raw data table
        cy.getByTestID('raw-data--toggle').click()
        cy.getByTestID('raw-data-table').should('exist')
        cy.getByTestID('raw-data--toggle').click()
      })

      it('can set min or max y-axis values', () => {
        // build the query to return data from beforeEach
        cy.getByTestID(`selector-list m`).click()
        cy.getByTestID('selector-list v').click()
        cy.getByTestID(`selector-list tv1`).click()

        cy.getByTestID('time-machine-submit-button').click()
        cy.getByTestID('cog-cell--button').click()
        cy.getByTestID('select-group--option')
          .contains('Custom')
          .click()
        cy.getByTestID('auto-domain--min')
          .type('-100')
          .blur()

        cy.getByTestID('form--element-error').should('not.exist')
        // find no errors
        cy.getByTestID('auto-domain--max')
          .type('450')
          .blur()
        // find no errors
        cy.getByTestID('form--element-error').should('not.exist')
        cy.getByTestID('auto-domain--min')
          .clear()
          .blur()
        cy.getByTestID('form--element-error').should('not.exist')
      })

      it('can view table data & sort values numerically', () => {
        // build the query to return data from beforeEach
        cy.getByTestID(`selector-list m`).click()
        cy.getByTestID('selector-list v').click()
        cy.getByTestID(`selector-list tv1`).click()
        cy.getByTestID('selector-list sort').click()

        cy.getByTestID('time-machine-submit-button').click()

        cy.getByTestID('view-type--dropdown').click()
        cy.getByTestID(`view-type--table`).click()
        // check to see that the FE rows are NOT sorted with flux sort
        cy.get('.table-graph-cell__sort-asc').should('not.exist')
        cy.get('.table-graph-cell__sort-desc').should('not.exist')
        cy.getByTestID('_value-table-header')
          .should('exist')
          .then(el => {
            // get the column index
            const columnIndex = el[0].getAttribute('data-column-index')
            let prev = -Infinity
            // get all the column values for that one and see if they are in order
            cy.get(`[data-column-index="${columnIndex}"]`).each(val => {
              const num = Number(val.text())
              if (isNaN(num) === false) {
                expect(num > prev).to.equal(true)
                prev = num
              }
            })
          })
        cy.getByTestID('_value-table-header').click()
        cy.get('.table-graph-cell__sort-asc').should('exist')
        cy.getByTestID('_value-table-header').click()
        cy.get('.table-graph-cell__sort-desc').should('exist')
        cy.getByTestID('_value-table-header').then(el => {
          // get the column index
          const columnIndex = el[0].getAttribute('data-column-index')
          let prev = Infinity
          // get all the column values for that one and see if they are in order
          cy.get(`[data-column-index="${columnIndex}"]`).each(val => {
            const num = Number(val.text())
            if (isNaN(num) === false) {
              expect(num < prev).to.equal(true)
              prev = num
            }
          })
        })
      })
    })
  })

  describe('saving', () => {
    beforeEach(() => {
      cy.fixture('routes').then(({orgs, explorer}) => {
        cy.get<Organization>('@org').then(({id}) => {
          cy.visit(`${orgs}/${id}${explorer}/save`)
        })
      })
    })

    describe('as a task', () => {
      beforeEach(() => {
        cy.getByTestID('task--radio-button').click()
      })

      it('should autoselect the first bucket', () => {
        cy.getByTestID('task-options-bucket-dropdown--button').within(() => {
          cy.get('span.cf-dropdown--selected').then(elem => {
            expect(elem.text()).to.include('defbuck')
          })
        })
      })
    })
  })

  // skipping until feature flag feature is removed for deleteWithPredicate
  describe.skip('delete with predicate', () => {
    beforeEach(() => {
      cy.getByTestID('delete-data-predicate').click()
      cy.getByTestID('overlay--container').should('have.length', 1)
    })

    it('requires consent to perform delete with predicate', () => {
      // confirm delete is disabled
      cy.getByTestID('confirm-delete-btn').should('be.disabled')
      // checks the consent input
      cy.getByTestID('delete-checkbox').check({force: true})
      // can delete
      cy.getByTestID('confirm-delete-btn')
        .should('not.be.disabled')
        .click()
    })

    it('should set the default bucket in the dropdown to the selected bucket', () => {
      cy.get('.cf-overlay--dismiss').click()
      cy.getByTestID('selector-list defbuck').click()
      cy.getByTestID('delete-data-predicate')
        .click()
        .then(() => {
          cy.getByTestID('dropdown--button').contains('defbuck')
          cy.get('.cf-overlay--dismiss').click()
        })
        .then(() => {
          cy.getByTestID('selector-list _monitoring').click()
          cy.getByTestID('delete-data-predicate')
            .click()
            .then(() => {
              cy.getByTestID('dropdown--button').contains('_monitoring')
              cy.get('.cf-overlay--dismiss').click()
            })
        })
        .then(() => {
          cy.getByTestID('selector-list _tasks').click()
          cy.getByTestID('delete-data-predicate')
            .click()
            .then(() => {
              cy.getByTestID('dropdown--button').contains('_tasks')
            })
        })
    })

    it('closes the overlay upon a successful delete with predicate submission', () => {
      cy.getByTestID('delete-checkbox').check({force: true})
      cy.getByTestID('confirm-delete-btn').click()
      cy.getByTestID('overlay--container').should('not.exist')
      cy.getByTestID('notification-success').should('have.length', 1)
    })
    // needs relevant data in order to test functionality
    it.skip('should require key-value pairs when deleting predicate with filters', () => {
      // confirm delete is disabled
      cy.getByTestID('add-filter-btn').click()
      // checks the consent input
      cy.getByTestID('delete-checkbox').check({force: true})
      // cannot delete
      cy.getByTestID('confirm-delete-btn').should('be.disabled')

      // should display warnings
      cy.getByTestID('form--element-error').should('have.length', 2)

      // TODO: add filter values based on dropdown selection in key / value
    })
  })
})

const lines = (numLines = 3) => {
  // each line is 10 seconds before the previous line
  const offset_ms = 10_000
  const now = Date.now()
  const nanos_per_ms = '000000'

  const decendingValues = Array(numLines)
    .fill(0)
    .map((_, i) => i)
    .reverse()

  const incrementingTimes = decendingValues.map(val => {
    return now - offset_ms * val
  })

  return incrementingTimes.map((tm, i) => {
    return `m,tk1=tv1 v=${i + 1} ${tm}${nanos_per_ms}`
  })
}
