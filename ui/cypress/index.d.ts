/* eslint @typescript-eslint/no-unused-vars: "off" */
import {
  signin,
  setupUser,
  createDashboard,
  createCell,
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
  createToken,
  createDashboardTemplate,
  writeData,
  getByTestIDSubStr,
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
      createCell: typeof createCell
      createOrg: typeof createOrg
      flush: typeof flush
      getByTestID: typeof getByTestID
      getByInputName: typeof getByInputName
      getByTitle: typeof getByTitle
      getByTestIDSubStr: typeof getByTestIDSubStr
      createAndAddLabel: typeof createAndAddLabel
      createLabel: typeof createLabel
      createBucket: typeof createBucket
      createScraper: typeof createScraper
      fluxEqual: typeof fluxEqual
      createTelegraf: typeof createTelegraf
      createToken: typeof createToken
      writeData: typeof writeData
    }
  }
}
