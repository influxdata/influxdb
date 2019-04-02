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
  createAndAddLabel,
  createLabel,
  createBucket,
  createScraper,
  fluxEqual,
  createTelegraf,
  createDashboardTemplate,
} from './support/commands'

declare global {
  namespace Cypress {
    interface Chainable {
      signin: typeof signin
      setupUser: typeof setupUser
      createSource: typeof createSource
      createTask: typeof createTask
      createVariable: typeof createVariable
      createDashboardTemplate: typeof createDashboardTemplate
      createDashboard: typeof createDashboard
      createOrg: typeof createOrg
      flush: typeof flush
      getByTestID: typeof getByTestID
      getByInputName: typeof getByInputName
      getByTitle: typeof getByTitle
      createAndAddLabel: typeof createAndAddLabel
      createLabel: typeof createLabel
      createBucket: typeof createBucket
      createScraper: typeof createScraper
      fluxEqual: typeof fluxEqual
      createTelegraf: typeof createTelegraf
    }
  }
}
