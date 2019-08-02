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
      })
  })
})
