import {Doc} from 'codemirror'
import {Organization} from '../../src/types'
import {
  FROM,
  RANGE,
  MEAN,
  MATH_ABS,
  MATH_FLOOR,
  STRINGS_TITLE,
  STRINGS_TRIM,
} from '../../src/shared/constants/fluxFunctions'

interface HTMLElementCM extends HTMLElement {
  CodeMirror: {
    doc: CodeMirror.Doc
  }
}

type $CM = JQuery<HTMLElementCM>

describe('DataExplorer', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org).as('org')
      cy.createMapVariable(body.org.id)
    })

    cy.fixture('routes').then(({orgs, explorer}) => {
      cy.get<Organization>('@org').then(({id}) => {
        cy.visit(`${orgs}/${id}${explorer}`)
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
      cy.getByTestID('switch-to-script-editor').click()
      cy.getByTestID('flux-editor').should('exist')
      cy.getByTestID('flux-editor').within(() => {
        cy.get('textarea').type('yoyoyoyoyo', {force: true})
      })

      cy.getByTestID('switch-query-builder-confirm--button').click()

      cy.getByTestID('switch-query-builder-confirm--popover--contents').within(
        () => {
          cy.getByTestID('button').click()
        }
      )

      cy.getByTestID('query-builder').should('exist')
    })

    it('should display the popover when hovering', () => {
      cy.getByTestID('selector-list my_meas')
        .click()
        .then(() => {
          cy.getByTestID('selector-list my_field')
            .click()
            .then(() => {
              cy.getByTestID('switch-to-script-editor').click()
              cy.getByTestID('flux-editor').should('exist')

              cy.getByTestID('toolbar-popover--contents').should('not.exist')

              cy.getByTestID('flux-function aggregateWindow').trigger(
                'mouseover'
              )

              cy.getByTestID('toolbar-popover--contents').should('exist')
            })
        })
    })
  })

  describe('raw script editing', () => {
    beforeEach(() => {
      cy.getByTestID('switch-to-script-editor').click()
    })

    it('enables the submit button when a query is typed', () => {
      cy.getByTestID('time-machine-submit-button').should('be.disabled')

      cy.getByTestID('flux-editor').within(() => {
        cy.get('textarea').type('yo', {force: true})
        cy.getByTestID('time-machine-submit-button').should('not.be.disabled')
      })
    })

    it('disables submit when a query is deleted', () => {
      cy.getByTestID('time-machine--bottom').then(() => {
        cy.get('textarea').type('from(bucket: "foo")', {force: true})
        cy.getByTestID('time-machine-submit-button').should('not.be.disabled')
        cy.get('textarea').type('{selectall} {backspace}', {force: true})
      })

      cy.getByTestID('time-machine-submit-button').should('be.disabled')
    })

    it('imports the appropriate packages to build a query', () => {
      cy.getByTestID('functions-toolbar-tab').click()

      cy.get<$CM>('.CodeMirror').then($cm => {
        const cm = $cm[0].CodeMirror
        cy.wrap(cm.doc).as('flux')
        expect(cm.doc.getValue()).to.eq('')
      })

      cy.getByTestID('flux-function from').click()
      cy.getByTestID('flux-function range').click()
      cy.getByTestID('flux-function math.abs').click()
      cy.getByTestID('flux-function math.floor').click()
      cy.getByTestID('flux-function strings.title').click()
      cy.getByTestID('flux-function strings.trim').click()

      cy.get<Doc>('@flux').then(doc => {
        const actual = doc.getValue()
        const expected = `
        import"${STRINGS_TITLE.package}"
        import"${MATH_ABS.package}"
        ${FROM.example}|>
        ${RANGE.example}|>
        ${MATH_ABS.example}|>
        ${MATH_FLOOR.example}|>
        ${STRINGS_TITLE.example}|>
        ${STRINGS_TRIM.example}`

        cy.fluxEqual(actual, expected).should('be.true')
      })
    })

    it('can use the function selector to build a query', () => {
      cy.getByTestID('functions-toolbar-tab').click()

      cy.get<$CM>('.CodeMirror').then($cm => {
        const cm = $cm[0].CodeMirror
        cy.wrap(cm.doc).as('flux')
        expect(cm.doc.getValue()).to.eq('')
      })

      cy.getByTestID('flux-function from').click()

      cy.get<Doc>('@flux').then(doc => {
        const actual = doc.getValue()
        const expected = FROM.example

        cy.fluxEqual(actual, expected).should('be.true')
      })

      cy.getByTestID('flux-function range').click()

      cy.get<Doc>('@flux').then(doc => {
        const actual = doc.getValue()
        const expected = `${FROM.example}|>${RANGE.example}`

        cy.fluxEqual(actual, expected).should('be.true')
      })

      cy.getByTestID('flux-function mean').click()

      cy.get<Doc>('@flux').then(doc => {
        const actual = doc.getValue()
        const expected = `${FROM.example}|>${RANGE.example}|>${MEAN.example}`

        cy.fluxEqual(actual, expected).should('be.true')
      })
    })

    it('can filter aggregation functions by name from script editor mode', () => {
      cy.get('.cf-input-field').type('covariance')
      cy.getByTestID('toolbar-function').should('have.length', 1)
    })

    it('shows the empty state when the query returns no results', () => {
      cy.getByTestID('time-machine--bottom').within(() => {
        cy.get('textarea').type(
          `from(bucket: "defbuck")
  |> range(start: -10s)
  |> filter(fn: (r) => r._measurement == "no exist")`,
          {force: true}
        )
        cy.getByTestID('time-machine-submit-button').click()
      })

      cy.getByTestID('empty-graph--no-results').should('exist')
    })

    it('can save query as task even when it has a variable', () => {
      const taskName = 'tax'
      // begin flux
      cy.getByTestID('flux-editor').within(() => {
        cy.get('textarea').type(
          `from(bucket: "defbuck")
  |> range(start: -15m, stop: now())
  |> filter(fn: (r) => r._measurement == `,
          {force: true}
        )
      })

      cy.getByTestID('toolbar-tab').click()
      //insert variable name by clicking on variable
      cy.get('.variables-toolbar--label').click()
      // finish flux
      cy.getByTestID('flux-editor').within(() => {
        cy.get('textarea').type(`)`, {force: true})
      })

      cy.getByTestID('save-query-as').click()
      cy.get('#save-as-task').click()
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
        .click()
      cy.get('.query-tab').should('have.length', 1)
    })

    it('can remove a second query using tab context menu', () => {
      cy.get('.query-tab').trigger('contextmenu')
      cy.getByTestID('right-click--remove-tab').should('have.class', 'disabled')

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
          cy.get('textarea').type('from(', {force: true})
          cy.getByTestID('time-machine-submit-button').click()
        })

        cy.getByTestID('empty-graph--error').should('exist')
      })
    })
  })

  describe('delete with predicate', () => {
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

    it('closes the overlay upon a successful delete with predicate submission', () => {
      cy.getByTestID('delete-checkbox').check({force: true})
      cy.getByTestID('confirm-delete-btn').click()
      cy.getByTestID('overlay--container').should('not.exist')
      cy.getByTestID('notification-success').should('have.length', 1)
    })

    it('should require key-value pairs when deleting predicate with filters', () => {
      // confirm delete is disabled
      cy.getByTestID('add-filter-btn').click()
      // checks the consent input
      cy.getByTestID('delete-checkbox').check({force: true})
      // cannot delete
      cy.getByTestID('confirm-delete-btn').should('be.disabled')

      // should display warnings
      cy.getByTestID('form--element-error').should('have.length', 2)

      cy.getByTestID('key-input').type('mean')
      cy.getByTestID('value-input').type(100)

      cy.getByTestID('confirm-delete-btn')
        .should('not.be.disabled')
        .click()
    })
  })
})
