import {Doc} from 'codemirror'
import {Organization} from '@influxdata/influx'
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
    })

    cy.fixture('routes').then(({orgs, explorer}) => {
      cy.get<Organization>('@org').then(({id}) => {
        cy.visit(`${orgs}/${id}${explorer}`)
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
      cy.get('.input-field').type('covariance')
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
})
