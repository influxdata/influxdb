import {Doc} from 'codemirror'
import {Organization} from '@influxdata/influx'
import {FROM, RANGE, MEAN} from '../../src/shared/constants/fluxFunctions'

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

    cy.fixture('routes').then(({explorer}) => {
      cy.visit(explorer)
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
  |> range(start: timeRangeStart, stop: timeRangeStop)
  |> filter(fn: (r) => r._measurement == "no exist")`,
          {force: true}
        )
        cy.getByTestID('time-machine-submit-button').click()
      })

      cy.getByTestID('empty-graph-message').within(() => {
        cy.contains('No Results').should('exist')
      })
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

      cy.getByTestID('buckets--button').click()

      cy.getByTestID('dropdown--item defbuck').should('exist')
      cy.getByTestID('dropdown--item newBucket').should('exist')
    })
  })

  describe('visualizations', () => {
    describe('empty states', () => {
      it('shows an error if a query is syntactically invalid', () => {
        cy.getByTestID('switch-to-script-editor').click()

        cy.getByTestID('time-machine--bottom').within(() => {
          cy.get('textarea').type('from(', {force: true})
          cy.getByTestID('time-machine-submit-button').click()
        })

        cy.getByTestID('empty-graph-message').within(() => {
          cy.contains('Error').should('exist')
        })
      })
    })
  })
})
