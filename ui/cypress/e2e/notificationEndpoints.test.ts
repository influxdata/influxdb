describe('Notification Endpoints', () => {
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
    cy.getByTestID('alert-column--header create-endpoint').click()

    cy.getByTestID('endpoint-name--input')
      .clear()
      .type('An Endpoint Has No Name')

    cy.getByTestID('endpoint-description--textarea')
      .clear()
      .type(
        'A minute, an hour, a month. Notification Endpoint is certain. The time is not.'
      )

    cy.getByTestID('endpoint-change--dropdown')
      .click()
      .within(() => {
        cy.getByTestID('endpoint--dropdown-item pagerduty').click()
        cy.getByTestID('endpoint--dropdown--button').within(() => {
          cy.contains('Pagerditty')
        })
      })
  })
})
