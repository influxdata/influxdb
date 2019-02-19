import {
  signin,
  setupUser,
  createDashboard,
  createOrg,
  flush,
  getByDataTest,
  getByInputName,
  getByTitle,
} from './support/commands'

declare global {
  namespace Cypress {
    interface Chainable {
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
}
