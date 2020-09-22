describe('Community Templates', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')

      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/settings/templates`)
      })
    })
  })

  it('launches github when the browse community template button is clicked', () => {
    cy.window().then(win => {
      cy.stub(win, 'open').as('windowOpenSpy')
    })

    cy.getByTestID('browse-template-button').click()

    cy.get('@windowOpenSpy').should(
      'be.calledWith',
      'https://github.com/influxdata/community-templates#templates'
    )
  })

  it('displays an error when invalid data is submitted through the field', () => {
    //on empty
    cy.getByTestID('lookup-template-button').click()
    cy.getByTestID('notification-error').should('be.visible')

    //lookup template errors on github folder
    cy.getByTestID('lookup-template-input').type(
      'https://github.com/influxdata/community-templates/tree/master/kafka'
    )
    cy.getByTestID('lookup-template-button').click()
    cy.getByTestID('notification-error').should('be.visible')
  })

  describe('installing a community template from the web', () => {
    it('accepts raw github and pluralizes resources properly', () => {
      //The lookup template accepts github raw link
      cy.getByTestID('lookup-template-input').type(
        'https://raw.githubusercontent.com/influxdata/influxdb/master/pkger/testdata/dashboard_gauge.yml'
      )
      cy.getByTestID('lookup-template-button').click()
      cy.getByTestID('template-install-overlay').should('be.visible')

      //check that with 1 resource pluralization is correct
      cy.getByTestID('template-install-title').should(
        'contain',
        'will create 1 resource'
      )
      cy.getByTestID('template-install-title').should(
        'not.contain',
        'resources'
      )

      //check that no resources check lead to disabled install button
      cy.getByTestID('heading-Dashboards').click()
      cy.getByTestID('template-install-button').should('exist')
      cy.getByTestID('templates-toggle--dash-1').click()
      cy.getByTestID('template-install-button').should('not.exist')

      //and check that 0 resources pluralization is correct
      cy.getByTestID('template-install-title').should(
        'contain',
        'will create 0 resources'
      )
    })

    it('forces installation of variables and buckets but allows selecting all other resources', () => {
      cy.getByTestID('lookup-template-input').type(
        'https://github.com/influxdata/influxdb/blob/master/pkger/testdata/variables.yml'
      )
      cy.getByTestID('lookup-template-button').click()
      cy.getByTestID('template-install-overlay').should('be.visible')

      cy.getByTestID('template-install-title').should(
        'contain',
        'will create 4 resources'
      )

      // no unchecking of variables
      cy.getByTestID('heading-Variables').click()
      cy.getByTestID('templates-toggle--var-const-3').should('be.visible')
      cy.getByTestID('templates-toggle--var-const-3--input').should(
        'be.disabled'
      )
      cy.getByTestID('templates-toggle--var-const-3').click()
      cy.getByTestID('template-install-title').should(
        'contain',
        'will create 4 resources'
      )

      cy.get('.cf-overlay--dismiss').click()
      cy.getByTestID('lookup-template-input').clear()
      cy.getByTestID('lookup-template-input').type(
        'https://github.com/influxdata/influxdb/blob/master/pkger/testdata/bucket.yml'
      )
      cy.getByTestID('lookup-template-button').click()
      cy.getByTestID('template-install-overlay').should('be.visible')

      // no unchecking  of buckets
      cy.getByTestID('template-install-title').should(
        'contain',
        'will create 2 resources'
      )
      cy.getByTestID('heading-Buckets').click()
      cy.getByTestID('templates-toggle--rucket-11').should('be.visible')
      cy.getByTestID('template-install-title').should(
        'contain',
        'will create 2 resources'
      )
      cy.getByTestID('templates-toggle--rucket-11--input').should('be.disabled')

      cy.get('.cf-overlay--dismiss').click()
      cy.getByTestID('lookup-template-input').clear()
      cy.getByTestID('lookup-template-input').type(
        'https://github.com/influxdata/influxdb/blob/master/pkger/testdata/tasks.yml'
      )
      cy.getByTestID('lookup-template-button').click()
      cy.getByTestID('template-install-overlay').should('be.visible')

      // can check and uncheck other resources
      cy.getByTestID('template-install-title').should(
        'contain',
        'will create 3 resources'
      )
      cy.getByTestID('heading-Tasks').click()
      cy.getByTestID('templates-toggle--task-1').should('be.visible')
      cy.getByTestID('templates-toggle--task-1').click()
      cy.getByTestID('template-install-title').should(
        'contain',
        'will create 2 resources'
      )

      cy.getByTestID('template-install-button').click()
      cy.getByTestID('notification-success').should('be.visible')
      cy.getByTestID('installed-template-tasks').should('be.visible')
    })
  })

  describe('the behavior of installed templates', () => {
    beforeEach(() => {
      cy.getByTestID('lookup-template-input').type(
        'https://github.com/influxdata/influxdb/blob/master/pkger/testdata/dashboard.json'
      )
      cy.getByTestID('lookup-template-button').click()

      cy.getByTestID('template-install-overlay').should('be.visible')
      cy.getByTestID('template-install-button').should('exist')
      cy.getByTestID('template-install-button').click()
      cy.getByTestID('notification-success').should('be.visible')
      cy.getByTestID('installed-template-dashboard').should('be.visible')
    })

    it('will install separate, identical templates', () => {
      cy.getByTestID('lookup-template-input').clear()
      cy.getByTestID('lookup-template-input').type(
        'https://github.com/influxdata/influxdb/blob/master/pkger/testdata/dashboard.json'
      )
      cy.getByTestID('lookup-template-button').click()
      cy.getByTestID('template-install-overlay').should('be.visible')
      cy.getByTestID('template-install-button').should('exist')
      cy.getByTestID('template-install-button').click()
      cy.getByTestID('notification-success').should('be.visible')
      cy.getByTestID('installed-template-list').should('have', '2')
    })

    it('links out to areas of the application where template resources were installed', () => {
      cy.getByTestID('lookup-template-input').clear()
      cy.getByTestID('lookup-template-input').type(
        'https://github.com/influxdata/influxdb/blob/master/pkger/testdata/telegraf.yml'
      )
      cy.getByTestID('lookup-template-button').click()
      cy.getByTestID('template-install-button').click()

      cy.getByTestID('template-resource-link').click({multiple: true})
      // dashboard
      cy.get('.community-templates--resources-table')
        .contains('dash-1')
        .click()
      cy.url().should('include', 'dashboards')
      cy.go('back')

      cy.getByTestID('template-resource-link').click({multiple: true})
      // telegraf
      cy.get('.community-templates--resources-table')
        .contains('tele-2')
        .click()
      cy.url().should(
        'match',
        /.*\/load-data\/telegrafs\/[\w\d]+\/instructions/
      )
      cy.go('back')

      cy.getByTestID('template-resource-link').click({multiple: true})
      // label
      cy.get('.community-templates--resources-table')
        .contains('label-1')
        .click()
      cy.url().should('include', 'settings/labels')
      cy.go('back')
    })

    // this behavior should be cleaned up
    it.skip('takes you to github readme when you click on the Community Templates button', () => {
      cy.getByTestID('community-template-readme-overlay-button').click()
      cy.get('.markdown-format').should('contain', 'Setup Instructions')
    })

    it('deletes templates', () => {
      cy.getByTestID('template-delete-button-dashboard--button').click()
      cy.getByTestID('template-delete-button-dashboard--confirm-button').click()
      cy.getByTestID('installed-template-dashboard').should('not.be.visible')
    })
  })
})
