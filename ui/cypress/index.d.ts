/* eslint @typescript-eslint/no-unused-vars: "off" */
import {
  signin,
  setupUser,
  createDashboard,
  createOrg,
  createSource,
  flush,
  getByTestID,
  getByInputName,
  getByTitle,
  createTask,
  createVariable,
  createLabel,
  createBucket,
  createScraper,
  fluxEqual,
  createTelegraf,
} from './support/commands'

declare global {
  namespace Cypress {
    interface Chainable {
      signin: typeof signin
      setupUser: typeof setupUser
      createSource: typeof createSource
      createTask: typeof createTask
      createVariable: typeof createVariable
      createDashboard: typeof createDashboard
      createOrg: typeof createOrg
      flush: typeof flush
      getByTestID: typeof getByTestID
      getByInputName: typeof getByInputName
      getByTitle: typeof getByTitle
      createLabel: typeof createLabel
      createBucket: typeof createBucket
      createScraper: typeof createScraper
      fluxEqual: typeof fluxEqual
      createTelegraf: typeof createTelegraf
    }
  }
}
