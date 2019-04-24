import {Organization} from '@influxdata/influx'



describe('tokens', () => {

  let descs: string[];
  let stati: boolean[];
  let authData: {description: string , status: boolean}[]

  beforeEach(() => {
    cy.flush()

    descs = [];
    stati = [];
    authData = [];

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')

      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/tokens`)
      })

      cy.request('api/v2/authorizations').then((resp) => {

        //console.log("DEBUG " + JSON.stringify(resp.body.authorizations[0].description))

//        descs.push(resp.body.authorizations[0].description)
        authData.push({description: resp.body.authorizations[0].description,
          status: resp.body.authorizations[0].status === 'active'})

        cy.createToken(id, 'token test \u0950', 'active',
          [{action: 'write', resource: {type: 'views'}},
            {action: 'write', resource: {type: 'documents'}},
            {action: 'write', resource: {type: 'dashboards'}},
            {action: 'write', resource: {type: 'buckets'}}])

//        descs.push('token test \u0950')
//        stati.push(true)
        authData.push({description: 'token test \u0950', status: true})

        cy.createToken(id, 'token test 02', 'inactive',
          [{action: 'write', resource: {type: 'views'}},
            {action: 'write', resource: {type: 'documents'}},
            {action: 'write', resource: {type: 'dashboards'}},
            {action: 'write', resource: {type: 'buckets'}}])

//        descs.push('token test 02')
//        stati.push(false)
        authData.push({description: 'token test 02', status: false})


        cy.createToken(id, 'token test 03', 'active',
          [{action: 'read', resource: {type: 'views'}},
            {action: 'read', resource: {type: 'documents'}},
            {action: 'read', resource: {type: 'dashboards'}},
            {action: 'read', resource: {type: 'buckets'}}])

//        descs.push('token test 03')
//        stati.push(true)
        authData.push({description: 'token test 03', status: true})


//        console.log("DEBUG " + descs)
        console.log("DEBUG " + JSON.stringify(authData))


      })


    })
  })

  it('can list tokens', () => {

    cy.getByTestID('table-row').should('have.length', 4).then((rows) => {

      descs = descs.sort()
      authData = authData.sort((a,b) => a.description < b.description ? -1 : a.description > b.description ? 1 : 0)

      for(var i = 0; i < rows.length; i++){

        cy.getByTestID('editable-name').eq(i).children('a').children('span').should('have.text', authData[i].description)

        if(authData[i].status) {
          cy.getByTestID('slide-toggle').eq(i).should('have.class', 'active')
        }else{
          cy.getByTestID('slide-toggle').eq(i).should('not.have.class', 'active')
        }
      }

    })

  })

  it('can filter tokens', () => {

  })

  it('can deactivate a token', () => {

  })

  it('can delete a token', () => {

  })
})