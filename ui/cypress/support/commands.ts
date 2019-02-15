declare namespace Cypress {
  interface Chainable<Subject> {
    signin: typeof signin
    setupUser: typeof setupUser
    createDashboard: typeof createDashboard
    createOrg: typeof createOrg
    flush: typeof flush
    getByDataTest: typeof getByDataTest
    getByInputName: typeof getByInputName
    getByTitle: typeof getByTitle
  }
}

const signin = (): Cypress.Chainable => {
  return cy.fixture('user').then(user => {
    cy.request({
      method: 'POST',
      url: '/api/v2/signin',
      auth: {user: user.username, pass: user.password},
    })
  })
}

// createDashboard relies on an org fixture to be set
const createDashboard = (orgID: string): Cypress.Chainable => {
  return cy.request({
    method: 'POST',
    url: '/api/v2/dashboards',
    body: {
      name: 'test dashboard',
      orgID,
    },
  })
}

const createOrg = (): Cypress.Chainable => {
  return cy.request({
    method: 'POST',
    url: '/api/v2/orgs',
    body: {
      name: 'test org',
    },
  })
}

const createBucket = (): Cypress.Chainable => {
  return cy.request({
    method: 'POST',
    url: '/api/v2/buckets',
    body: {
      name: 'test org',
    },
  })
}

// TODO: have to go through setup because we cannot create a user w/ a password via the user API
const setupUser = (): Cypress.Chainable => {
  return cy.fixture('user').then(({username, password, org, bucket}) => {
    return cy.request({
      method: 'POST',
      url: '/api/v2/setup',
      body: {username, password, org, bucket},
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

const getByTitle = (name: string): Cypress.Chainable => {
  return cy.get(`[title=${name}]`)
}

// getters
Cypress.Commands.add('getByDataTest', getByDataTest)
Cypress.Commands.add('getByInputName', getByInputName)
Cypress.Commands.add('getByTitle', getByTitle)

// auth flow
Cypress.Commands.add('signin', signin)

// setup
Cypress.Commands.add('setupUser', setupUser)

// dashboards
Cypress.Commands.add('createDashboard', createDashboard)

// orgs
Cypress.Commands.add('createOrg', createOrg)

// buckets
Cypress.Commands.add('createBucket', createBucket)

// general
Cypress.Commands.add('flush', flush)
