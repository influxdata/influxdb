declare namespace Cypress {
  interface Chainable<Subject> {
    signin: typeof signin
    getByDataTest: typeof getByDataTest
    getByInputName: typeof getByInputName
    createUser: typeof createUser
    flush: typeof flush
    createDashboard: typeof createDashboard
  }
}

const signin = (): Cypress.Chainable => {
  return cy.fixture('user').then(user => {
    cy.createUser().then(() => {
      cy.request({
        method: 'POST',
        url: '/api/v2/signin',
        auth: {user: user.username, pass: user.password},
      })
    })
  })
}

// createDashboard relies on an org fixture to be set
const createDashboard = (): Cypress.Chainable => {
  return cy.fixture('org').then(({id}) => {
    return cy.request({
      method: 'POST',
      url: '/api/v2/dashboards',
      body: {
        name: 'test dashboard',
        orgID: id,
      },
    })
  })
}

// TODO: have to go through setup because we cannot create a user w/ a password via the user API
const createUser = (): Cypress.Chainable => {
  return cy.fixture('user').then(({username, password, org, bucket}) => {
    cy.request({
      method: 'POST',
      url: '/api/v2/setup',
      body: {username, password, org, bucket},
    }).then(({body}) => {
      cy.writeFile('cypress/fixtures/org.json', body.org)
    })
  })
}

const flush = () => {
  cy.request({
    method: 'GET',
    url: '/debug/flush',
  })
}

// DOM node getters
const getByDataTest = (dataTest: string): Cypress.Chainable => {
  return cy.get(`[data-testid="${dataTest}"]`)
}

const getByInputName = (name: string): Cypress.Chainable => {
  return cy.get(`input[name=${name}]`)
}

// getters
Cypress.Commands.add('getByDataTest', getByDataTest)
Cypress.Commands.add('getByInputName', getByInputName)

// auth flow
Cypress.Commands.add('signin', signin)

// users
Cypress.Commands.add('createUser', createUser)

// dashboards
Cypress.Commands.add('createDashboard', createDashboard)

// general
Cypress.Commands.add('flush', flush)
