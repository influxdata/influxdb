import {Organization} from '@influxdata/influx'



describe('tokens', () => {

  let authData: {description: string , status: boolean, id: string}[]
  let testTokens: {id: string, description: string, status: string, permissions: object[]}[]

  beforeEach(() => {
    cy.flush()

    authData = [];

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')
      console.log("DEBUG session cookie " + JSON.stringify(cy.getCookie('foobar')))

      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/tokens`)
      })

//      cy.server().route('POST', 'api/v2/authorizations').as('createToken')

      testTokens = [{id: id, description: 'token test \u0950', status: 'active', permissions: [{action: 'write', resource: {type: 'views'}},
          {action: 'write', resource: {type: 'documents'}},
          {action: 'write', resource: {type: 'dashboards'}},
          {action: 'write', resource: {type: 'buckets'}}]},
        {id: id, description: 'token test 02', status: 'inactive', permissions: [{action: 'write', resource: {type: 'views'}},
            {action: 'write', resource: {type: 'documents'}},
            {action: 'write', resource: {type: 'dashboards'}},
            {action: 'write', resource: {type: 'buckets'}}]},
        {id: id, description: 'token test 03', status: 'inactive', permissions: [{action: 'read', resource: {type: 'views'}},
            {action: 'read', resource: {type: 'documents'}},
            {action: 'read', resource: {type: 'dashboards'}},
            {action: 'read', resource: {type: 'buckets'}}]},
         ]


      cy.request('api/v2/authorizations').then((resp) => {

        authData.push({description: resp.body.authorizations[0].description,
          status: resp.body.authorizations[0].status === 'active', id: resp.body.authorizations[0].id})

        //console.log("DEBUG cookies " + JSON.stringify(cy.getCookies()))

        testTokens.forEach((token) => {


          //fetch('http://localhost:9999/api/v2/authorizations?orgID=' + id, {
          //  method: 'POST', body: JSON.stringify(token), headers: { 'Content-Type': 'application/json', 'session':  } })

          cy.createToken(token.id, token.description, token.status, token.permissions).then((resp) => {
            authData.push({description: resp.body.description, status: resp.body.status === 'active', id: resp.body.id})
          })

//          cy.wait("@createToken")

        })

      })

    })
  })

  it('can list tokens', () => {

    cy.getByTestID('table-row').should('have.length', 4).then((rows) => {

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

    //basic filter
    cy.getByTestID('input-field--filter').type('test')
    cy.getByTestID('table-row').should('have.length', 3)

    //clear filter
    cy.getByTestID('input-field--filter').clear()
    cy.getByTestID('table-row').should('have.length', 4)

    //exotic filter
    cy.getByTestID('input-field--filter').type('\u0950')
    cy.getByTestID('table-row').should('have.length', 1)

  })

  it('can change token activation status', () => {

    //toggle on
    cy.getByTestID('table-row').contains('token test 02').parents('[data-testid=table-row]').within(() => {
       cy.getByTestID('slide-toggle').click().then(() => {

         //wait for backend to sync
         cy.wait(1000)

         //check for status update on backend
         // @ts-ignore
         cy.request('api/v2/authorizations/' + authData.find(function(item){
            return item.description === 'token test 02'
         }).id).then((resp) => {

           expect(resp.body.status).equals('active')

         })

       })

    })

    cy.getByTestID("table-row").contains('token test 02').parents('[data-testid=table-row]').within(() => {
      cy.getByTestID('slide-toggle').should('have.class', 'active')
    })

    cy.getByTestID('table-row').contains('token test 02').parents('[data-testid=table-row]').within(() => {
      cy.getByTestID('slide-toggle').click().then(() => {

        //wait for backend to sync
        cy.wait(1000)

        //check for status update on backend
        // @ts-ignore
        cy.request('api/v2/authorizations/' + authData.find(function(item){
          return item.description === 'token test 02'
        }).id).then((resp) => {

          expect(resp.body.status).equals('inactive')

        })
      })
    })

    cy.getByTestID("table-row").contains('token test 02').parents('[data-testid=table-row]').within(() => {
      cy.getByTestID('slide-toggle').should('not.have.class', 'active')
    })

  })

  it('can delete a token', () => {

    cy.getByTestID('table-row').should('have.length', 4)

    cy.getByTestID('table-row').contains('token test 03').parents('[data-testid=table-row]').within(() => {
      cy.getByTestID('delete-button').click().then(() => {
        cy.getByTestID('confirmation-button').click({force: true});
      })
    })

    cy.getByTestID('table-row').should('have.length', 3)

    cy.getByTestID('table-row').contains('token test 03').should('not.exist')

  })

  it('can generate a token', () => {

  })
})