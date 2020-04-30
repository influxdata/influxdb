import {Organization} from '../../src/types'
interface ResourceIDs {
  orgID: string
  dbID: string
  cellID: string
}

const generateRandomSixDigitNumber = () => {
  const digits = []
  for (let i = 0; i < 6; i++) {
    digits[i] = Math.floor(Math.random() * Math.floor(10))
  }
  return Number(digits.join(''))
}

describe('The Query Builder', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org).as('org')
    })

    cy.writeData([
      `mem,host=thrillbo-swaggins active=${generateRandomSixDigitNumber()}`,
      `mem,host=thrillbo-swaggins cached=${generateRandomSixDigitNumber()}`,

      `mem,host=thrillbo-swaggins active=${generateRandomSixDigitNumber()}`,
      `mem,host=thrillbo-swaggins cached=${generateRandomSixDigitNumber()}`,
    ])
  })

  describe('from the Data Explorer', () => {
    it('creates a query, edits it to add another field, then views its results with pride and satisfaction', () => {
      cy.get('@org').then((org: Organization) => {
        cy.visit(`orgs/${org.id}/data-explorer`)
      })

      cy.contains('mem').click('right') // users sometimes click in random spots
      cy.contains('active').click('bottomLeft')

      cy.getByTestID('empty-graph--no-queries').should('exist')

      cy.contains('Submit').click()

      cy.get('.giraffe-plot').should('exist')

      cy.getByTestID('save-query-as').click()

      // open the dashboard selector dropdown
      cy.getByTestID('save-as-dashboard-cell--dropdown').click()
      cy.getByTestID('save-as-dashboard-cell--create-new-dash').click()
      cy.getByTestID('save-as-overlay--header').click() // close the blast door i mean dropdown

      cy.getByTestID('save-as-dashboard-cell--dashboard-name').type(
        'Basic Ole Dashboard'
      )
      cy.getByTestID('save-as-dashboard-cell--cell-name').type('Memory Usage')
      cy.getByTestID('save-as-dashboard-cell--submit').click()

      // wait for the notification since it's highly animated
      // we close the notification since it contains the name of the dashboard and interfers with cy.contains
      cy.wait(250)
      cy.get('.cf-notification--dismiss').click()
      cy.wait(250)

      // force a click on the hidden dashboard nav item (cypress can't do the hover)
      // i assure you i spent a nonzero amount of time trying to do this the way a user would
      cy.getByTestID('nav-item-dashboards').click()

      cy.contains('Basic Ole Dashboard').click()
      cy.getByTestID('cell-context--toggle').click()
      cy.contains('Configure').click()

      cy.get('.giraffe-plot').should('exist')
      cy.contains('cached').click()
      cy.contains('mean').click()
      cy.contains('Submit').click()
      cy.getByTestID('save-cell--button').click()

      cy.getByTestID('nav-item-dashboards').click()

      cy.contains('Basic Ole Dashboard').click()
      cy.getByTestID('cell-context--toggle').click()
      cy.contains('Configure').click()

      cy.get('.giraffe-plot').should('exist')
      cy.getByTestID('cancel-cell-edit--button').click()
      cy.contains('Basic Ole Dashboard').should('exist')
    })

    it('can create a bucket from the buckets list', () => {
      cy.get('@org').then((org: Organization) => {
        cy.visit(`orgs/${org.id}/data-explorer`)
      })

      const newBucketName = '٩(｡•́‿•̀｡)۶'

      cy.getByTestID('selector-list add-bucket').click()

      cy.getByTestID('bucket-form').should('exist')

      cy.getByTestID('bucket-form-name').type(newBucketName)

      cy.getByTestID('bucket-form-submit').click()

      cy.getByTestID('buckets-list').within(() => {
        cy.contains(newBucketName).should('exist')
      })
    })
  })

  describe('the group() function', () => {
    it('creates a query that has a group() function in it', () => {
      cy.get('@org').then((org: Organization) => {
        cy.visit(`orgs/${org.id}/data-explorer`)
      })

      cy.contains('mem').click('left')
      cy.getByTestID('builder-card')
        .last()
        .contains('Filter')
        .click()
        .get('.cf-dropdown--menu-container')
        .contains('group')
        .click()

      const groupableColums = []

      cy.getByTestID('builder-card')
        .last()
        .then($lastBuilderCard => {
          $lastBuilderCard.find('.selector-list--item').each((index, $item) => {
            groupableColums.push($item.innerHTML)
          })

          expect(groupableColums).to.eql([
            '_start',
            '_stop',
            '_time',
            '_measurement',
            '_field',
          ])
        })
    })
  })

  // This is flaky in prod
  // https://circleci.com/gh/influxdata/influxdb/74628#artifacts/containers/0
  describe.skip('from the Dashboard view', () => {
    beforeEach(() => {
      cy.get('@org').then((org: Organization) => {
        cy.createDashboard(org.id).then(({body}) => {
          cy.createCell(body.id).then(cellResp => {
            const dbID = body.id
            const orgID = org.id
            const cellID = cellResp.body.id
            cy.createView(dbID, cellID)
            cy.wrap({orgID, dbID, cellID}).as('resourceIDs')
          })
        })
      })
    })

    it("creates a query, edits the query, edits the cell's default name, edits it again, submits with the keyboard, then chills", () => {
      cy.get<ResourceIDs>('@resourceIDs').then(({orgID, dbID, cellID}) => {
        cy.visit(`orgs/${orgID}/dashboards/${dbID}/cells/${cellID}/edit`)
      })

      // build query
      cy.contains('mem').click('topLeft') // users sometimes click in random spots
      cy.contains('cached').click('bottomLeft')
      cy.contains('thrillbo-swaggins').click('left')
      cy.contains('sum').click()

      cy.getByTestID('empty-graph--no-queries').should('exist')
      cy.contains('Submit').click()
      cy.getByTestID('giraffe-layer-line').should('exist')
      cy.getByTestID('overlay')
        .contains('Name this Cell')
        .click()
      cy.get('[placeholder="Name this Cell"]').type('A better name!')
      cy.get('.veo-contents').click() // click out of inline editor
      cy.getByTestID('save-cell--button').click()

      // A race condition exists between saving the cell's updated name and re-opening the cell.
      // Will replace this with a cy.wait(@updateCell) when Cypress supports
      // waiting on window.fetch responses: https://github.com/cypress-io/cypress/issues/95
      // resolves: https://github.com/influxdata/influxdb/issues/16141

      cy.get<ResourceIDs>('@resourceIDs').then(({orgID, dbID, cellID}) => {
        cy.visit(`orgs/${orgID}/dashboards/${dbID}/cells/${cellID}/edit`)
      })

      cy.getByTestID('giraffe-layer-line').should('exist')
      cy.getByTestID('overlay')
        .contains('A better name!')
        .click()

      cy.get('[placeholder="Name this Cell"]').type(
        "Uncle Moe's Family Feedbag{enter}"
      )
      cy.getByTestID('save-cell--button').click()
    })
  })
})
