/* eslint @typescript-eslint/no-unused-vars: "off" */
import 'jest'

import {
  signin,
  setupUser,
  createDashboard,
  createCell,
  createOrg,
  createSource,
  deleteOrg,
  flush,
  getByTestID,
  getByInputName,
  getByInputValue,
  getByTitle,
  createTask,
  createMapVariable,
  createCSVVariable,
  createQueryVariable,
  createAndAddLabel,
  createLabel,
  createBucket,
  createScraper,
  createView,
  fluxEqual,
  createTelegraf,
  createToken,
  createDashboardTemplate,
  writeData,
  getByTestIDSubStr,
  createEndpoint,
  createDashWithCell,
  createDashWithViewAndVar,
} from './support/commands'

declare global {
  namespace Cypress {
    interface Chainable {
      signin: typeof signin
      setupUser: typeof setupUser
      createSource: typeof createSource
      createCSVVariable: typeof createCSVVariable
      createQueryVariable: typeof createQueryVariable
      createTask: typeof createTask
      createMapVariable: typeof createMapVariable
      createDashboardTemplate: typeof createDashboardTemplate
      createDashboard: typeof createDashboard
      createCell: typeof createCell
      createDashWithCell: typeof createDashWithCell
      createDashWithViewAndVar: typeof createDashWithViewAndVar
      createView: typeof createView
      createOrg: typeof createOrg
      deleteOrg: typeof deleteOrg
      flush: typeof flush
      getByTestID: typeof getByTestID
      getByInputName: typeof getByInputName
      getByInputValue: typeof getByInputValue
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
      createEndpoint: typeof createEndpoint
    }
  }
}
