describe('DataExplorer', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin()

    cy.fixture('routes').then(({explorer}) => {
      cy.visit(explorer)
    })
  })

  describe('raw script editing', () => {
    it('enables the submit button when a query is typed', () => {
      cy.getByTestID('switch-to-script-editor').click()

      cy.getByTestID('time-machine-submit-button').should('be.disabled')

      cy.getByTestID('flux-editor').within(() => {
        cy.get('textarea').type('yo', {force: true})
        cy.getByTestID('time-machine-submit-button').should('not.be.disabled')
      })
    })

    it('disables submit when a query is deleted', () => {
      cy.getByTestID('switch-to-script-editor').click()

      cy.getByTestID('time-machine--bottom').then(() => {
        cy.get('textarea').type('from(bucket: "foo")', {force: true})
        cy.getByTestID('time-machine-submit-button').should('not.be.disabled')
        cy.get('textarea').type('{selectall} {backspace}', {force: true})
      })

      cy.getByTestID('time-machine-submit-button').should('be.disabled')
    })

    it('can filter aggregation functions by name from script editor mode', () => {
      cy.getByTestID('switch-to-script-editor').click()

      cy.get('.input-field').type('covariance')
      cy.getByTestID('toolbar-function').should('have.length', 1)
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

      it('show an empty state for tag keys when the bucket is empty', () => {
        cy.getByTestID('empty-tag-keys').should('exist')
      })
    })
  })
})
