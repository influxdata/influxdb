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

  it('can view the index page', () => {
    cy.getByTestID('flows-index').within(() => {
      cy.getByTestID('create-flow--button empty').click()
    })
  })
})
