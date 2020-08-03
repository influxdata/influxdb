import {Organization} from '../../src/types'

// a generous commitment to delivering this page in a loaded state
const PAGE_LOAD_SLA = 10000
const all_access_token_name = 'the prince and the frog'

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
    cy.get('.cf-resource-card').should('have.length', 4)
    cy.getByTestID('token-list').then(rows => {
      authData = authData.sort((a, b) =>
        // eslint-disable-next-line
        a.description < b.description
          ? -1
          : a.description > b.description
          ? 1
          : 0
      )

      for (let i = 0; i < rows.length; i++) {
        cy.getByTestID(`token-name ${authData[i].description}`)
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
    cy.get('.cf-resource-card').should('have.length', 3)

    // clear filter
    cy.getByTestID('input-field--filter').clear()
    cy.get('.cf-resource-card').should('have.length', 4)

    // exotic filter
    cy.getByTestID('input-field--filter').type('\u0950')
    cy.get('.cf-resource-card').should('have.length', 1)
  })

  it('can change token activation status', () => {
    // toggle on
    cy.getByTestID('token-card token test 02').within(() => {
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

    cy.getByTestID('token-card token test 02').within(() => {
      cy.getByTestID('slide-toggle').should('have.class', 'active')
    })

    cy.getByTestID('token-card token test 02').within(() => {
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

    cy.getByTestID('token-card token test 02').within(() => {
      cy.getByTestID('slide-toggle').should('not.have.class', 'active')
    })
  })

  it('can delete a token', () => {
    cy.get('.cf-resource-card').should('have.length', 4)

    cy.getByTestID('token-card token test 03').within(() => {
      cy.getByTestID('context-menu').click()

      cy.getByTestID('delete-token')
        .contains('Delete')
        .click()
    })

    cy.get('.cf-resource-card').should('have.length', 3)

    cy.getByTestID('resource-card token test 03').should('not.exist')

    // Delete remaining tokens
    cy.get('.cf-resource-card')
      .first()
      .within(() => {
        cy.getByTestID('context-menu').click()

        cy.getByTestID('delete-token')
          .contains('Delete')
          .click()
      })

    cy.get('.cf-resource-card')
      .first()
      .within(() => {
        cy.getByTestID('context-menu').click()

        cy.getByTestID('delete-token')
          .contains('Delete')
          .click()
      })

    cy.get('.cf-resource-card')
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

  it('can generate a all access token', () => {
    cy.get('.cf-resource-card').should('have.length', 4)

    // open overlay
    cy.getByTestID('dropdown-button--gen-token').click()
    cy.getByTestIDSubStr('dropdown-item').should('have.length', 2)
    cy.getByTestID('dropdown-item generate-token--all-access').click()
    cy.getByTestID('overlay--container').should('be.visible')

    //create token
    cy.getByTestID('all-access-token-input').type(all_access_token_name)
    cy.getByTestID('button--save').click()

    cy.getByTestID(`token-card ${all_access_token_name}`).should('be.visible')
  })

  it('can generate a read/write token', () => {
    cy.get('.cf-resource-card').should('have.length', 4)

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
      cy.get('.cf-resource-card').should('have.length', 4)

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
      cy.get('.cf-resource-card').should('have.length', 5)

      cy.getByTestID('token-name Jeton 01').click()

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
      cy.getByTestID('token-card token test \u0950').click()

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

  it('can edit the description', () => {
    cy.getByTestID('token-card token test 02').within(() => {
      cy.getByTestID('resource-editable-name--button').click()
      cy.getByTestID('resource-editable-name--input').type(
        'renamed-description{enter}'
      )
    })
    cy.getByTestID('token-card renamed-description').should('be.visible')
  })

  it('can do sorting', () => {
    cy.getByTestID("token-card u1's Token").within(() => {
      cy.getByTestID('context-menu').click()

      cy.getByTestID('delete-token')
        .contains('Delete')
        .click()
    })

    // description asc
    cy.getByTestID('resource-sorter--button')
      .click()
      .then(() => {
        cy.getByTestID('resource-sorter--description-asc').click()
      })
      .then(() => {
        const sortedtoken = testTokens.sort((a, b) =>
          a.description > b.description ? 1 : -1
        )
        cy.get('[data-testid*="token-card"]').each((val, index) => {
          const testID = val.attr('data-testid')
          expect(testID).to.contain(sortedtoken[index].description)
        })
      })

    //description desc
    cy.getByTestID('resource-sorter--button')
      .click()
      .then(() => {
        cy.getByTestID('resource-sorter--description-desc').click()
      })
      .then(() => {
        const sortedtoken = testTokens
          .sort((a, b) => (a.description > b.description ? 1 : -1))
          .reverse()
        cy.get('[data-testid*="token-card"]').each((val, index) => {
          const testID = val.attr('data-testid')
          expect(testID).to.contain(sortedtoken[index].description)
        })
      })

    //status asc
    cy.getByTestID('resource-sorter--button')
      .click()
      .then(() => {
        cy.getByTestID('resource-sorter--status-asc').click()
      })
      .then(() => {
        const sortedtoken = testTokens.sort((a, b) =>
          a.status > b.status ? 1 : -1
        )
        cy.get('[data-testid*="token-card"]').each((val, index) => {
          const testID = val.attr('data-testid')
          expect(testID).to.contain(sortedtoken[index].description)
        })
      })

    //status desc
    cy.getByTestID('resource-sorter--button')
      .click()
      .then(() => {
        cy.getByTestID('resource-sorter--status-desc').click()
      })
      .then(() => {
        const sortedtoken = testTokens
          .sort((a, b) => (a.status > b.status ? 1 : -1))
          .reverse()
        cy.get('[data-testid*="token-card"]').each((val, index) => {
          const testID = val.attr('data-testid')
          expect(testID).to.contain(sortedtoken[index].description)
        })
      })
  })

  it('can Select all buckets in Generate Read/Write token', () => {
    // create some extra buckets
    cy.get<Organization>('@org').then(({id, name}: Organization) => {
      cy.createBucket(id, name, 'Magna Carta').then(() => {
        cy.createBucket(id, name, 'Sicilsky Bull').then(() => {
          cy.createBucket(id, name, 'A la Carta')
        })
      })
    })

    //"Select all" button function test
    // open overlay
    cy.getByTestID('dropdown-button--gen-token').click()
    cy.getByTestIDSubStr('dropdown-item').should('have.length', 2)
    cy.getByTestID('dropdown-item generate-token--read-write').click()
    cy.getByTestID('overlay--container').should('be.visible')

    //input token description
    cy.getByTestID('input-field--descr')
      .clear()
      .type('Select all test')
      .should('have.value', 'Select all test')

    //select all buckets and save
    cy.getByTestID('grid--column')
      .eq(0)
      .within(() => {
        cy.getByTestID('selector-list--header').within(() => {
          cy.getByTitle('Select All').click()
        })
      })

    cy.getByTestID('grid--column')
      .eq(1)
      .within(() => {
        cy.getByTestID('selector-list--header').within(() => {
          cy.getByTitle('Select All').click()
        })
      })

    //save and assert the notification
    cy.getByTestID('button--save')
      .click()
      .then(() => {
        cy.getByTestID('notification-success--children').contains(
          'Token was created successfully'
        )
      })

    cy.getByTestID('token-card Select all test')

    //"All buckets" button function test
    // open overlay
    cy.getByTestID('dropdown-button--gen-token').click()
    cy.getByTestID('dropdown-item generate-token--read-write').click()
    cy.getByTestID('overlay--container').should('be.visible')

    //input token description
    cy.getByTestID('input-field--descr')
      .clear()
      .type('All buckets test')
      .should('have.value', 'All buckets test')

    //select all buckets with "All buckets" button and save
    cy.getByTestID('select-group')
      .eq(0)
      .within(() => {
        cy.getByTestID('select-group--option')
          .contains('All Buckets')
          .click()
      })

    cy.getByTestID('select-group')
      .eq(1)
      .within(() => {
        cy.getByTestID('select-group--option')
          .contains('All Buckets')
          .click()
      })

    //save and assert the notification
    cy.getByTestID('button--save')
      .click()
      .then(() => {
        cy.getByTestID('notification-success--children').contains(
          'Token was created successfully'
        )
      })

    cy.getByTestID('token-name All buckets test').click()

    //assert that there is read and write permission
    cy.getByTestID('permissions-section')
      .eq(0)
      .contains('write')
    cy.getByTestID('permissions-section')
      .eq(0)
      .contains('read')

    cy.get('.cf-overlay--dismiss').click()

    //select all - only write permission
    // open overlay
    cy.getByTestID('dropdown-button--gen-token').click()
    cy.getByTestIDSubStr('dropdown-item').should('have.length', 2)
    cy.getByTestID('dropdown-item generate-token--read-write').click()
    cy.getByTestID('overlay--container').should('be.visible')

    //input token description
    cy.getByTestID('input-field--descr')
      .clear()
      .type('Write only test')
      .should('have.value', 'Write only test')

    //select all buckets
    cy.getByTestID('grid--column')
      .eq(0)
      .within(() => {
        cy.getByTestID('selector-list--header').within(() => {
          cy.getByTitle('Select All').click()
        })
      })

    cy.getByTestID('grid--column')
      .eq(1)
      .within(() => {
        cy.getByTestID('selector-list--header').within(() => {
          cy.getByTitle('Select All').click()
        })
      })

    //deselect buckets in the read column
    cy.getByTestID('grid--column')
      .eq(0)
      .within(() => {
        cy.getByTestID('selector-list--header').within(() => {
          cy.getByTitle('Deselect All').click()
        })
      })

    //save and assert the notification
    cy.getByTestID('button--save')
      .click()
      .then(() => {
        cy.getByTestID('notification-success--children').contains(
          'Token was created successfully'
        )
      })

    cy.getByTestID('token-name Write only test').click()

    //assert that there is only write permission
    cy.getByTestID('permissions-section')
      .eq(0)
      .contains('write')
      .then(() => {
        cy.getByTestID('permissions-section')
          .eq(0)
          .within(() => {
            cy.getByTestID('permissions--item')
              .contains('read')
              .should('not.exist')
          })
      })

    cy.get('.cf-overlay--dismiss').click()

    //select all - only read permission
    // open overlay
    cy.getByTestID('dropdown-button--gen-token').click()
    cy.getByTestIDSubStr('dropdown-item').should('have.length', 2)
    cy.getByTestID('dropdown-item generate-token--read-write').click()
    cy.getByTestID('overlay--container').should('be.visible')

    //input token description
    cy.getByTestID('input-field--descr')
      .clear()
      .type('Read only test')
      .should('have.value', 'Read only test')

    //select all buckets
    cy.getByTestID('grid--column')
      .eq(0)
      .within(() => {
        cy.getByTestID('selector-list--header').within(() => {
          cy.getByTitle('Select All').click()
        })
      })

    cy.getByTestID('grid--column')
      .eq(1)
      .within(() => {
        cy.getByTestID('selector-list--header').within(() => {
          cy.getByTitle('Select All').click()
        })
      })

    //deselect buckets in the read column
    cy.getByTestID('grid--column')
      .eq(1)
      .within(() => {
        cy.getByTestID('selector-list--header').within(() => {
          cy.getByTitle('Deselect All').click()
        })
      })

    //save and assert the notification
    cy.getByTestID('button--save')
      .click()
      .then(() => {
        cy.getByTestID('notification-success--children').contains(
          'Token was created successfully'
        )
      })

    cy.getByTestID('token-name Read only test').click()

    //assert that there is only write permission
    cy.getByTestID('permissions-section')
      .eq(0)
      .contains('read')
      .then(() => {
        cy.getByTestID('permissions-section')
          .eq(0)
          .within(() => {
            cy.getByTestID('permissions--item')
              .contains('write')
              .should('not.exist')
          })
      })
  })
})
