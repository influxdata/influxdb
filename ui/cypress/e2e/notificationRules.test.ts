describe('NotificationRules', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')
      cy.fixture('routes').then(({orgs, alerting}) => {
        cy.visit(`${orgs}/${id}${alerting}`)
      })
    })
  })

  it('can create a notification rule', () => {
    cy.getByTestID('open-create-rule--button').click()

    cy.getByTestID('rule-name--input').type('my-new-rule')

    // Rule schedule section
    cy.getByTestID('rule-schedule-cron').click()
    cy.getByTestID('rule-schedule-cron--input')
      .type('2 0 * * *')
      .should('have.value', '2 0 * * *')
    cy.getByTestID('rule-schedule-every').click()
    cy.getByTestID('rule-schedule-every--input')
      .type('20m')
      .should('have.value', '20m')
    cy.getByTestID('rule-schedule-offset--input')
      .type('1m')
      .should('have.value', '1m')

    // Rule Conditions section
    cy.getByTestID('status-rule').should('have.length', 1)
    cy.getByTestID('add-status-rule--button').click()
    cy.getByTestID('status-rule').should('have.length', 2)

    // Editing a Status Rule
    cy.getByTestID('status-rule')
      .first()
      .within(() => {
        cy.getByTestID('count--input')
          .type('{uparrow}')
          .should('have.value', '2')
        cy.getByTestID('period--input')
          .clear()
          .type('3m')
          .should('have.value', '3m')

        cy.getByTestID('status-change--dropdown')
          .click()
          .within(() => {
            cy.getByTestID('status-change--dropdown-item equal').click()
            cy.getByTestID('status-change--dropdown--button').within(() => {
              cy.contains('equal')
            })
          })

        cy.getByTestID('levels--dropdown previousLevel').should('not.exist')
        cy.getByTestID('levels--dropdown currentLevel').should('exist')

        cy.getByTestID('status-change--dropdown')
          .click()
          .within(() => {
            cy.getByTestID('status-change--dropdown-item changes from').click()
            cy.getByTestID('status-change--dropdown--button').within(() => {
              cy.contains('changes from')
            })
          })

        cy.getByTestID('levels--dropdown previousLevel').click()
        cy.getByTestID('levels--dropdown-item INFO').click()
        cy.getByTestID('levels--dropdown--button previousLevel').within(() => {
          cy.contains('INFO')
        })

        cy.getByTestID('levels--dropdown currentLevel').click()
        cy.getByTestID('levels--dropdown-item CRIT').click()
        cy.getByTestID('levels--dropdown--button currentLevel').within(() => {
          cy.contains('CRIT')
        })
      })
  })
})
