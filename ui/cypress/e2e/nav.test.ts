describe('navigation', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org.id).as('orgID')
    })

    cy.visit('/')
  })

  it('can navigate to each page', () => {
    // Load Data Page
    cy.getByTestID('nav-item-load-data').click()
    cy.getByTestID('load-data--header').should('exist')

    // Data Explorer Page
    cy.getByTestID('nav-item-data-explorer').click()
    cy.getByTestID('data-explorer--header').should('exist')

    // Dashboard Index Page
    cy.getByTestID('nav-item-dashboards').click()
    cy.getByTestID('empty-dashboards-list').should('exist')

    // Tasks Index Page
    cy.getByTestID('nav-item-tasks').click()
    cy.getByTestID('tasks-page--header').should('exist')

    // Alerts Page
    cy.getByTestID('nav-item-alerting').click()
    cy.getByTestID('alerts-page--header').should('exist')

    // Settings Page
    cy.getByTestID('nav-item-settings').click()
    cy.getByTestID('settings-page--header').should('exist')

    // Home Page
    cy.getByTestID('tree-nav--header').click()
    cy.getByTestID('home-page--header').should('exist')

    // User Nav -- Members
    cy.getByTestID('user-nav').click()
    cy.getByTestID('user-nav-item-members').click()
    cy.getByTestID('member-page--header').should('exist')
    cy.url().should('contain', 'members')

    // User Nav -- About
    cy.getByTestID('user-nav').click()
    cy.getByTestID('user-nav-item-about').click()
    cy.getByTestID('member-page--header').should('exist')
    cy.url().should('contain', 'about')

    // User Nav -- Switch Orgs
    cy.getByTestID('user-nav').click()
    cy.getByTestID('user-nav-item-switch-orgs').click()
    cy.getByTestID('switch-overlay--header').should('exist')
    cy.get('.cf-overlay--dismiss').click()

    // User Nav -- Create Orgs
    cy.getByTestID('user-nav').click()
    cy.getByTestID('user-nav-item-create-orgs').click()
    cy.getByTestID('create-org-overlay--header').should('exist')
    cy.get('.cf-overlay--dismiss').click()

    // User Nav -- Log Out
    cy.getByTestID('user-nav').click()
    cy.getByTestID('user-nav-item-logout').click()
    cy.getByTestID('signin-page').should('exist')
  })
})
