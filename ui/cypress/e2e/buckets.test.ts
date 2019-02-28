import {Bucket, Organization} from '@influxdata/influx'

describe('Buckets', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id, name},
        bucket,
      } = body
      cy.wrap(body.org).as('org')
      cy.wrap(bucket).as('bucket')
      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/buckets_tab`)
      })
    })
  })

  describe('from the org view', () => {
    it('can create a bucket', () => {
      const newBucket = '🅱️ucket'
      cy.getByTestID('table-row').should('have.length', 1)

      cy.contains('Create').click()
      cy.getByTestID('overlay--container').within(() => {
        cy.getByInputName('name').type(newBucket)
        cy.get('.button')
          .contains('Create')
          .click()
      })

      cy.getByTestID('table-row')
        .should('have.length', 2)
        .and('contain', newBucket)
    })

    it('can update a buckets name and retention rules', () => {
      const newName = 'newdefbuck'

      cy.get<Bucket>('@bucket').then(({name}) => {
        cy.contains(name).click()
      })

      cy.getByTestID('retention-intervals').click()

      cy.getByInputName('days').type('{uparrow}')
      cy.getByInputName('hours').type('{uparrow}')
      cy.getByInputName('minutes').type('{uparrow}')
      cy.getByInputName('seconds').type('{uparrow}')

      cy.getByTestID('overlay--container').within(() => {
        cy.getByInputName('name')
          .clear()
          .type(newName)

        cy.contains('Save').click()
      })

      cy.getByTestID('table-row')
        .should('contain', '1 day')
        .and('contain', newName)
    })

    it('can delete a bucket', () => {
      cy.get<Organization>('@org').then(({id, name}) => {
        cy.createBucket(id, name, 'newbucket1')
        cy.createBucket(id, name, 'newbucket2')
      })

      cy.getByTestID('table-row').should('have.length', 3)

      cy.getByTestID('confirmation-button')
        .last()
        .click({force: true})

      cy.getByTestID('table-row').should('have.length', 2)
    })
  })
})
