/* eslint @typescript-eslint/no-unused-vars: "off" */
import {
  signin,
  setupUser,
  createDashboard,
  createOrg,
  createSource,
  flush,
  getByDataTest,
  getByInputName,
  getByTitle,
  createTask,
} from './support/commands'

declare global {
  namespace Cypress {
    interface Chainable {
      signin: typeof signin
      setupUser: typeof setupUser
      createSource: typeof createSource
      createTask: typeof createTask
      createDashboard: typeof createDashboard
      createOrg: typeof createOrg
      flush: typeof flush
      getByDataTest: typeof getByDataTest
      getByInputName: typeof getByInputName
      getByTitle: typeof getByTitle
    }
  }
}
