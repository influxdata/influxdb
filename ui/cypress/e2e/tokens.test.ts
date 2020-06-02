import {Organization} from '../../src/types'

// a generous commitment to delivering this page in a loaded state
const PAGE_LOAD_SLA = 10000

describe('tokens', () => {
  let authData: {description: string; status: boolean; id: string}[]
  let testTokens: {
    id: string
    description: string
    status: string
    permissions: object[]
  }[]

  beforeEach(() => {
    cy.flush()

    authData = []

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')

      testTokens = [
        {
          id: id,
          description: 'token test \u0950',
          status: 'active',
          permissions: [
            {action: 'write', resource: {type: 'views'}},
            {action: 'write', resource: {type: 'documents'}},
            {action: 'write', resource: {type: 'dashboards'}},
            {action: 'write', resource: {type: 'buckets'}},
          ],
        },
        {
          id: id,
          description: 'token test 02',
          status: 'inactive',
          permissions: [
            {action: 'write', resource: {type: 'views'}},
            {action: 'write', resource: {type: 'documents'}},
            {action: 'write', resource: {type: 'dashboards'}},
            {action: 'write', resource: {type: 'buckets'}},
          ],
        },
        {
          id: id,
          description: 'token test 03',
          status: 'inactive',
          permissions: [
            {action: 'read', resource: {type: 'views'}},
            {action: 'read', resource: {type: 'documents'}},
            {action: 'read', resource: {type: 'dashboards'}},
            {action: 'read', resource: {type: 'buckets'}},
          ],
        },
      ]

      // check out array.reduce for the nested calls here
      cy.request('api/v2/authorizations').then(resp => {
        expect(resp.body).to.exist
        authData.push({
          description: resp.body.authorizations[0].description,
          status: resp.body.authorizations[0].status === 'active',
          id: resp.body.authorizations[0].id,
        })

        testTokens.forEach(token => {
          cy.createToken(
            token.id,
            token.description,
            token.status,
            token.permissions
          ).then(resp => {
            expect(resp.body).to.exist
            authData.push({
              description: resp.body.description,
              status: resp.body.status === 'active',
              id: resp.body.id,
            })
          })
        })

        cy.fixture('routes').then(({orgs}) => {
          cy.visit(`${orgs}/${id}/load-data/tokens`)
        })
        cy.get('[data-testid="resource-list"]', {timeout: PAGE_LOAD_SLA})
      })
    })
  })

  it('can list tokens', () => {
    cy.getByTestID('resource-card').should('have.length', 4)
    cy.getByTestID('resource-card').then(rows => {
      authData = authData.sort((a, b) =>
        // eslint-disable-next-line
        a.description < b.description
          ? -1
          : a.description > b.description
          ? 1
          : 0
      )

      for (let i = 0; i < rows.length; i++) {
        cy.getByTestID('resource-editable-name')
          .eq(i)
          .contains(authData[i].description)

        if (authData[i].status) {
          cy.getByTestID('slide-toggle')
            .eq(i)
            .should('have.class', 'active')
        } else {
          cy.getByTestID('slide-toggle')
            .eq(i)
            .should('not.have.class', 'active')
        }
      }
    })
  })

  it('can filter tokens', () => {
    // basic filter
    cy.getByTestID('input-field--filter').type('test')
    cy.getByTestID('resource-card').should('have.length', 3)

    // clear filter
    cy.getByTestID('input-field--filter').clear()
    cy.getByTestID('resource-card').should('have.length', 4)

    // exotic filter
    cy.getByTestID('input-field--filter').type('\u0950')
    cy.getByTestID('resource-card').should('have.length', 1)
  })

  it('can change token activation status', () => {
    // toggle on
    cy.getByTestID('resource-card')
      .contains('token test 02')
      .parents('[data-testid=resource-card]')
      .within(() => {
        cy.getByTestID('slide-toggle')
          .click()
          .then(() => {
            // wait for backend to sync
            cy.wait(1000)

            // check for status update on backend
            cy.request(
              'api/v2/authorizations/' +
                (authData.find(function(item) {
                  return item.description === 'token test 02'
                }) as any).id
            ).then(resp => {
              expect(resp.body.status).equals('active')
            })
          })
      })

    cy.getByTestID('resource-card')
      .contains('token test 02')
      .parents('[data-testid=resource-card]')
      .within(() => {
        cy.getByTestID('slide-toggle').should('have.class', 'active')
      })

    cy.getByTestID('resource-card')
      .contains('token test 02')
      .parents('[data-testid=resource-card]')
      .within(() => {
        cy.getByTestID('slide-toggle')
          .click()
          .then(() => {
            // wait for backend to sync
            cy.wait(1000)

            // check for status update on backend
            cy.request(
              'api/v2/authorizations/' +
                (authData.find(function(item) {
                  return item.description === 'token test 02'
                }) as any).id
            ).then(resp => {
              expect(resp.body.status).equals('inactive')
            })
          })
      })

    cy.getByTestID('resource-card')
      .contains('token test 02')
      .parents('[data-testid=resource-card]')
      .within(() => {
        cy.getByTestID('slide-toggle').should('not.have.class', 'active')
      })
  })

  it('can delete a token', () => {
    cy.getByTestID('resource-card').should('have.length', 4)

    cy.getByTestID('resource-card')
      .contains('token test 03')
      .parents('[data-testid=resource-card]')
      .within(() => {
        cy.getByTestID('context-menu').click()

        cy.getByTestID('delete-token')
          .contains('Delete')
          .click()
      })

    cy.getByTestID('resource-card').should('have.length', 3)

    cy.getByTestID('resource-card')
      .contains('token test 03')
      .should('not.exist')

    // Delete remaining tokens
    cy.getByTestID('resource-card')
      .first()
      .within(() => {
        cy.getByTestID('context-menu').click()

        cy.getByTestID('delete-token')
          .contains('Delete')
          .click()
      })

    cy.getByTestID('resource-card')
      .first()
      .within(() => {
        cy.getByTestID('context-menu').click()

        cy.getByTestID('delete-token')
          .contains('Delete')
          .click()
      })

    cy.getByTestID('resource-card')
      .first()
      .within(() => {
        cy.getByTestID('context-menu').click()

        cy.getByTestID('delete-token')
          .contains('Delete')
          .click()
      })

    // Assert empty state
    cy.getByTestID('empty-state').within(() => {
      cy.getByTestID('dropdown--gen-token').should('exist')
    })
  })

  it('can generate a read/write token', () => {
    cy.getByTestID('resource-card').should('have.length', 4)

    // create some extra buckets for filters
    cy.get<Organization>('@org').then(({id, name}: Organization) => {
      cy.createBucket(id, name, 'Magna Carta').then(() => {
        cy.createBucket(id, name, 'Sicilsky Bull').then(() => {
          cy.createBucket(id, name, 'A la Carta')
        })
      })

      // open overlay
      cy.getByTestID('dropdown-button--gen-token').click()
      cy.getByTestIDSubStr('dropdown-item').should('have.length', 2)
      cy.getByTestID('dropdown-item generate-token--read-write').click()
      cy.getByTestID('overlay--container').should('be.visible')

      // check cancel
      cy.getByTestID('button--cancel').click()
      cy.getByTestID('overlay--container').should('not.be.visible')
      cy.getByTestID('resource-card').should('have.length', 4)

      // open overlay - again
      cy.getByTestID('dropdown-button--gen-token').click()
      cy.getByTestIDSubStr('dropdown-item').should('have.length', 2)
      cy.getByTestID('dropdown-item generate-token--read-write').click()
      cy.getByTestID('overlay--container').should('be.visible')

      // Create a token  //todo filters in this or seperate test
      cy.getByTestID('input-field--descr').type('Jeton 01')
      cy.getByTestID('builder-card--body')
        .eq(0)
        .within(() => {
          cy.getByTitle('Click to filter by Sicilsky Bull').click()
          cy.getByTitle('Click to filter by A la Carta').click()
        })
      cy.getByTestID('builder-card--body')
        .eq(1)
        .within(() => {
          cy.getByTitle('Click to filter by Sicilsky Bull').click()
        })

      cy.getByTestID('button--save').click()

      // Verify token
      cy.getByTestID('resource-card')
        .should('have.length', 5)
        .contains('Jeton 01')
        .should('be.visible')
      cy.getByTestID('resource-card')
        .contains('Jeton 01')
        .click()
      cy.getByTestID('overlay--container').should('be.visible')
      cy.getByTestID('overlay--header').should('contain', 'Jeton 01')
      cy.getByTestID('permissions-section').should('have.length', 2)

      cy.getByTestID('permissions-section')
        .contains('buckets-Sicilsky Bull')
        .should('be.visible')
      cy.getByTestID('permissions-section')
        .contains('buckets-Sicilsky Bull')
        .parent()
        .parent()
        .within(() => {
          cy.getByTestID('permissions--item').should('have.length', 2)
          cy.getByTestID('permissions--item')
            .contains('write')
            .should('be.visible')
          cy.getByTestID('permissions--item')
            .contains('read')
            .should('be.visible')
        })

      cy.getByTestID('permissions-section')
        .contains('buckets-A la Carta')
        .parent()
        .parent()
        .within(() => {
          cy.getByTestID('permissions--item').should('have.length', 1)
          cy.getByTestID('permissions--item')
            .contains('write')
            .should('not.be.visible')
          cy.getByTestID('permissions--item')
            .contains('read')
            .should('be.visible')
        })
    })

    it('can view a token', () => {
      cy.getByTestID('resource-card')
        .contains('token test \u0950')
        .click()

      // title match
      cy.getByTestID('overlay--container').should('be.visible')
      cy.getByTestID('overlay--header').should('contain', 'token test \u0950')

      // summary match
      cy.getByTestID('permissions-section').should('have.length', 4)
      cy.getByTestID('permissions-section')
        .contains('views')
        .should('be.visible')
      cy.getByTestID('permissions-section')
        .contains('documents')
        .should('be.visible')
      cy.getByTestID('permissions-section')
        .contains('dashboards')
        .should('be.visible')
      cy.getByTestID('permissions-section')
        .contains('buckets')
        .should('be.visible')

      // copy to clipboard + notification
      cy.getByTestID('button-copy').click()
      cy.getByTestID('notification-success').should($msg => {
        expect($msg).to.contain('has been copied to clipboard')
      })
      // todo check system clipboard

      // close button
      cy.getByTestID('overlay--header').within(() => {
        cy.get('button').click()
      })
      cy.getByTestID('overlay--container').should('not.be.visible')
    })
  })
})
