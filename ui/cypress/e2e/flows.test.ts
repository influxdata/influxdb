describe('Flows', () => {
  beforeEach(() => {
    cy.flush()
    cy.signin().then(({body}) => {
      const {
        org: {id},
        bucket,
      } = body
      cy.wrap(body.org).as('org')
      cy.wrap(bucket).as('bucket')
      cy.fixture('routes').then(({orgs, flows}) => {
        cy.visit(`${orgs}/${id}${flows}`)
      })
    })
  })

  // TODO: write e2e tests when this is real
  it.skip('CRUD a flow from the index page', () => {
    cy.getByTestID('create-flow--button')
      .first()
      .click()
  })
})
