import {Organization} from '@influxdata/influx'

describe('labels', () => {

  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')

      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/labels`)
      })
    })
  })

  function hex2BgColor(hex: string): string {
    hex = hex.replace('#', '')
    let subvals: string[] = hex.match(/.{1,2}/g)
    console.log('DEBUG ' + subvals)
    let red: number = parseInt(subvals[0], 16)
    let green: number = parseInt(subvals[1], 16)
    let blue: number = parseInt(subvals[2], 16)
    console.log('DEBUG ' + red + " " + green + " " + blue)
    //background-color: rgb(50, 107, 186);

    return `background-color: rgb(${red}, ${green}, ${blue});`
  }

  it('Can create a label', () => {
    const newLabelName = 'Substantia (サブスタンス)'
    const newLabelDescription = '(\u03943) quod in se est et per se concipitur hoc est id cujus conceptus non indiget conceptu alterius rei a quo formari debeat. '
    const newLabelColor = '#D4AF37'

    cy.getByTestID('table-row').should('have.length', 0)

    //open create - first button
    cy.getByTestID('button-create-initial').click()

    cy.getByTestID('overlay--container').within(() => {
      cy.getByTestID('overlay--header').contains('Create Label').should('be.visible')
      //dismiss
      cy.getByTestID('overlay--header').children('button').click()
    })

    cy.getByTestID('overlay--container').should('not.be.visible')

    //open create 2 - by standard button
    cy.getByTestID('button-create').click()
    cy.getByTestID('overlay--container').should('be.visible')

    //cancel
    cy.getByTestID('create-label-form--cancel').click()
    cy.getByTestID('overlay--container').should('not.be.visible')
    cy.getByTestID('label-card').should('have.length', 0)

    //open create - and proceed with overlay
    cy.getByTestID('button-create-initial').click()

    //Try to save without name (required field) todo - issue 13940
    //cy.getByTestID('create-label-form--submit').click()

    //enter name
    cy.getByTestID('create-label-form--name').type(newLabelName)
    //enter description
    cy.getByTestID('create-label-form--description').type(newLabelDescription)
    //select color
    cy.getByTestID('color-picker--input').invoke('attr', 'value').should('contain','#326BBA')
    cy.getByTestID('color-picker--swatch').should('have.length', 50)
    cy.getByTestID('color-picker--swatch').eq(23).trigger('mouseover')
    cy.getByTestID('color-picker--swatch').eq(23).invoke('attr', 'title').should('contain','Honeydew')
    cy.getByTestID('color-picker--swatch').eq(33).trigger('mouseover')
    cy.getByTestID('color-picker--swatch').eq(33).invoke('attr', 'title').should('contain','Thunder')
    cy.getByTestID('color-picker--swatch').eq(33).click();
    cy.getByTestID('color-picker--input').invoke('attr','value').should('equal','#FFD255')
    cy.getByTestID('color-picker--input').parent().parent().children('div.color-picker--selected')
      .invoke('attr', 'style').should('equal','background-color: rgb(255, 210, 85);')


    //clear color select
    cy.getByTestID('color-picker--input').clear()
    cy.getByTestID('form--element-error').should('contain', 'Hexcodes must begin with #, and must be 7 characters')
    cy.getByTestID('input-error').should( $ie => {
       expect($ie).to.have.class('alert-triangle')
    })
    cy.getByTestID('input-error').parent().parent().children('div.color-picker--selected').invoke('attr', 'style').should('equal','')

    //Type nonsense string - color input
    cy.getByTestID('color-picker--input').type('zzzzzz')
    cy.getByTestID('form--element-error').should('contain', 'Hexcodes must begin with #, and must be 7 characters')
    cy.getByTestID('input-error').should( $ie => {
      expect($ie).to.have.class('alert-triangle')
    })
    cy.getByTestID('input-error').parent().parent().children('div.color-picker--selected').invoke('attr', 'style').should('equal','')

    //feel lucky
    cy.getByTestID('color-picker--randomize').click()
    cy.getByTestID('color-picker--input').invoke('val').then( hex => {
      cy.getByTestID('color-picker--input').parent().parent().children('div.color-picker--selected')
        .invoke('attr', 'style').should('equal',hex2BgColor(hex))
    })
    //enter color
    cy.getByTestID('color-picker--input').clear()
    cy.getByTestID('color-picker--input').type(newLabelColor)
    cy.getByTestID('color-picker--input').invoke('val').then( hex => {
      cy.getByTestID('color-picker--input').parent().parent().children('div.color-picker--selected')
        .invoke('attr', 'style').should('equal',hex2BgColor(newLabelColor))
    })

    //save
    cy.getByTestID('create-label-form--submit').click()

    //verify name, descr, color
    cy.getByTestID('label-card').should('have.length', 1)
    cy.getByTestID('label-card').contains(newLabelName).should('be.visible')
    cy.getByTestID('label-card').contains(newLabelDescription).should('be.visible')
    cy.getByTestID('label-card').children('div.resource-list--name-meta').children('div.label')
      .invoke('attr', 'style').should('contain', hex2BgColor(newLabelColor))

  })

  it('can update a label', () => {

    const oldLabelName = 'attributum (атрибут)'
    const oldLabelDescription = '(\u03944) Per attributum intelligo id quod intellectus de substantia percipit tanquam ejusdem essentiam constituens. '
    const oldLabelColor = '#D0D0F8'

    const newLabelName = 'attribut (атрибут)'
    const newLabelDescription = '(\u03944) J\'entends par attribut ce que l\'entendement perçoit d\'une substance comme constituant son essence. '
    const newLabelColor = '#B0D0FF'

  })

  it('can delete a label', () => {
    const labelName = 'Modus (目录)'
    const labelDesicription = '(\u03945) Per modum intelligo substantiae affectiones sive id quod in alio est, per quod etiam concipitur.'
  })

})