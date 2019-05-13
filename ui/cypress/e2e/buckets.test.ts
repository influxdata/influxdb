import {Bucket, Organization} from '@influxdata/influx'

describe('Buckets', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
        bucket,
      } = body
      cy.wrap(body.org).as('org')
      cy.wrap(bucket).as('bucket')
      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/buckets`)
      })
    })
  })

  describe('from the org view', () => {
    it('can create a bucket', () => {
      const newBucket = '🅱️ucket'
      cy.getByTestID('resource-card').should('have.length', 1)

      cy.getByTestID('Create Bucket').click()
      cy.getByTestID('overlay--container').within(() => {
        cy.getByInputName('name').type(newBucket)
        cy.get('.button')
          .contains('Create')
          .click()
      })

      cy.getByTestID('resource-card')
        .should('have.length', 2)
        .and('contain', newBucket)
    })

    it("can update a bucket's retention rules", () => {
      cy.get<Bucket>('@bucket').then(({name}) => {
        cy.contains(name).click()
      })

      cy.contains('Periodically').click()
      // Switch back to line 47 when radio buttons from clockface support testID
      // cy.get('retention-intervals').click()

      cy.getByInputName('days').type('{uparrow}')
      cy.getByInputName('hours').type('{uparrow}')
      cy.getByInputName('minutes').type('{uparrow}')
      cy.getByInputName('seconds').type('{uparrow}')

      cy.getByTestID('overlay--container').within(() => {
        cy.contains('Save').click()
      })

      cy.getByTestID('resource-card').should('contain', '1 day')
    })

    it.skip('can delete a bucket', () => {
      cy.get<Organization>('@org').then(({id, name}) => {
        cy.createBucket(id, name, 'newbucket1')
        cy.createBucket(id, name, 'newbucket2')
      })

      cy.getByTestID('resource-card').should('have.length', 3)

      cy.getByTestID('confirmation-button')
        .last()
        .click({force: true})

      cy.getByTestID('resource-card').should('have.length', 2)
    })
  })
})
