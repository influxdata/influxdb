// Covers creating cells and managing view options
describe('cells veo', () => {
  let nowNano = new Date().getTime() * 1000000
  let intervalNano: number = 600 * 1000 * 1000000 //10 min in nanosecs
  let lines: string[] = []
  let recCount = 256
  let startTime = nowNano - recCount * intervalNano

  let dbId: string
  let cellId: string

  for (let i = 0; i < recCount; i++) {
    lines[i] =
      'mymeas hodnota=' +
      (Math.cos(i) * 50.0 + 50.0) +
      ' ' +
      (startTime + i * intervalNano)
  }

  function canvasCheckSum(canvas: HTMLCanvasElement): number {
    let ctx = canvas.getContext('2d')
    // @ts-ignore
    let imgData = ctx.getImageData(0, 0, canvas.width, canvas.height)
    let sum = 0
    for (let i = 0; i < imgData.data.length; i += 4) {
      sum +=
        imgData.data[i] +
        imgData.data[i + 1] +
        imgData.data[i + 2] +
        imgData.data[i + 3]
    }

    return sum
  }

  /*
     This helper function is needed because the URL directly into the
     Cell Edit page no longer works

     e.g.  http://localhost:9999/orgs/04094d93652b8000/dashboards/04094d944ceb8000/cells/04094d948bab8000/edit?lower=now%28%29%20-%205m

     Also some tests check the edit cycle directly from the dashboard view
   */
  function openVisualizationsEditor() {
    cy.getByTestID('cell--view-empty').click()

    cy.getByTestID('context-menu')
      .eq(0)
      .click({force: true})
    cy.contains('Configure').click({force: true})
    cy.getByTestID('button--vis-opts').click()
  }

  beforeEach(() => {
    cy.flush()
    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')
      cy.writeData(lines)

      cy.createDashboard(id).then(({body}) => {
        //cy.wrap(body.id).as('dbId')
        dbId = body.id
        cy.createCell(body.id, {x: 0, y: 0, height: 4, width: 4}, 'blah').then(
          ({body}) => {
            //cy.wrap(body.id).as('cellId')
            cellId = body.id

            cy.createView(dbId, cellId).then(() => {
              cy.fixture('routes').then(({orgs}) => {
                cy.visit(
                  //`${orgs}/${id}/dashboards/${dbId}/cells/${cellId}/edit?lower=now%28%29%20-%205m`
                  `${orgs}/${id}/dashboards/${dbId}`
                )
              })
            })
          }
        )
      })
    })
  })

  //todo - further investigate snapshot visual comparisons - issue with timestamp (x-axis) differences between images
  describe('Graph Visualizations', () => {
    it('can change the line interpolation to Smooth', () => {
      openVisualizationsEditor()

      cy.getByTestID('giraffe-layer--line')
        .eq(1)
        .then(jqcanvas1 => {
          let sum1 = canvasCheckSum(jqcanvas1.get(0) as HTMLCanvasElement)

          cy.contains('Linear').click()
          cy.contains('Smooth').click()

          cy.getByTestID('giraffe-layer--line')
            .eq(1)
            .then(jqcanvas2 => {
              let sum2 = canvasCheckSum(jqcanvas2.get(0) as HTMLCanvasElement)

              expect(sum2).not.equals(sum1)
            })
        })
    })

    it('can change the line interpolation to Step', () => {
      openVisualizationsEditor()

      cy.getByTestID('giraffe-layer--line')
        .eq(1)
        .then(jqcanvas1 => {
          let sum1 = canvasCheckSum(jqcanvas1.get(0) as HTMLCanvasElement)

          cy.contains('Linear').click()
          cy.contains('Step').click()

          cy.getByTestID('giraffe-layer--line')
            .eq(1)
            .then(jqcanvas2 => {
              let sum2 = canvasCheckSum(jqcanvas2.get(0) as HTMLCanvasElement)

              expect(sum2).not.equals(sum1)
            })
        })
    })

    it('can change the line colors to Delorean', () => {
      openVisualizationsEditor()
      cy.getByTestID('giraffe-layer--line')
        .eq(1)
        .then(jqcanvas1 => {
          let sum1 = canvasCheckSum(jqcanvas1.get(0) as HTMLCanvasElement)

          cy.contains('Nineteen Eighty Four').click()
          cy.contains('Delorean').click()

          cy.getByTestID('giraffe-layer--line')
            .eq(1)
            .then(jqcanvas2 => {
              let sum2 = canvasCheckSum(jqcanvas2.get(0) as HTMLCanvasElement)

              expect(sum2).not.equals(sum1)
            })
        })
    })

    it('can modify the Y axis label', () => {
      //for some reason the axes canvas is now considered hidden by cypress

      openVisualizationsEditor()

      cy.document().then(doc1 => {
        // N.B. Cypress functions .get etc. maintain the canvas element is not visible
        // and  Cypress then refuses to work with it
        // This is a workaround
        const canvas1 = doc1.getElementsByClassName(
          'giraffe-axes'
        )[1] as HTMLCanvasElement
        let sum1 = canvasCheckSum(canvas1)

        cy.getByTestID('input--y-axis-label').type('DVORNIK')

        cy.document().then(doc2 => {
          const canvas2 = doc2.getElementsByClassName(
            'giraffe-axes'
          )[1] as HTMLCanvasElement
          let sum2 = canvasCheckSum(canvas2)
          expect(sum2).not.equals(sum1)
        })
      })
    })

    it('can modify the Y tick prefix', () => {
      //for some reason the axes canvas is now considered hidden by cypress

      openVisualizationsEditor()

      cy.document().then(doc1 => {
        // N.B. Cypress functions .get etc. maintain the canvas element is not visible
        // and  Cypress then refuses to work with it
        // This is a workaround
        const canvas1 = doc1.getElementsByClassName(
          'giraffe-axes'
        )[1] as HTMLCanvasElement
        let sum1 = canvasCheckSum(canvas1)

        cy.getByTestID('input--Y-axis-pref').type('над')

        cy.document().then(doc2 => {
          const canvas2 = doc2.getElementsByClassName(
            'giraffe-axes'
          )[1] as HTMLCanvasElement
          let sum2 = canvasCheckSum(canvas2)
          expect(sum2).not.equals(sum1)
        })
      })
    })

    it('can modify the T tick suffix', () => {
      //for some reason the axes canvas is now considered hidden by cypress

      openVisualizationsEditor()

      cy.document().then(doc1 => {
        // N.B. Cypress functions .get etc. maintain the canvas element is not visible
        // and  Cypress then refuses to work with it
        // This is a workaround
        const canvas1 = doc1.getElementsByClassName(
          'giraffe-axes'
        )[1] as HTMLCanvasElement
        let sum1 = canvasCheckSum(canvas1)

        cy.getByTestID('input--Y-axis-suff').type('под')

        cy.document().then(doc2 => {
          const canvas2 = doc2.getElementsByClassName(
            'giraffe-axes'
          )[1] as HTMLCanvasElement
          let sum2 = canvasCheckSum(canvas2)
          expect(sum2).not.equals(sum1)
        })
      })
    })

    it('can set a custom Y axis domain', () => {
      //for some reason the axes canvas is now considered hidden by cypress

      openVisualizationsEditor()

      cy.document().then(doc1 => {
        // N.B. Cypress functions .get etc. maintain the canvas element is not visible
        // and  Cypress then refuses to work with it
        // This is a workaround
        const canvas1 = doc1.getElementsByClassName(
          'giraffe-axes'
        )[1] as HTMLCanvasElement
        let sum1 = canvasCheckSum(canvas1)

        cy.getByTestID('radio-button--custom').click()
        cy.getByTestID('input--min-val').type('-10{enter}')
        cy.getByTestID('form--element-error').should(
          'have.text',
          'Must supply a valid maximum value'
        )
        cy.getByTestID('input--max-val').type('110{enter}')

        cy.document().then(doc2 => {
          const canvas2 = doc2.getElementsByClassName(
            'giraffe-axes'
          )[1] as HTMLCanvasElement
          let sum2 = canvasCheckSum(canvas2)
          expect(sum2).not.equals(sum1)
        })
      })
    })

    //comprehensive test of above - begin in dashboard view, modify everything, save and check cell in dashboard
    it('can modify the dashboard view of the cell', () => {
      let sumInit: number
      let sumFinal: number

      cy.contains('TEST CELL').should('be.visible')
      cy.getByTestID('giraffe-axes').should('be.visible')
      cy.getByTestID('giraffe-layer--line').should('be.visible')

      cy.getByTestID('giraffe-layer--line').then(jqCanvas1 => {
        sumInit = canvasCheckSum(jqCanvas1.get(0) as HTMLCanvasElement)

        cy.getByTestID('context-menu')
          .eq(0)
          .click()
        cy.getByTestID('context-menu-item')
          .eq(0)
          .click()
        cy.getByTestID('button--vis-opts').click()
        cy.contains('Linear').click()
        cy.contains('Step').click()
        cy.contains('Nineteen Eighty Four').click()
        cy.contains('Delorean').click()
        cy.getByTestID('input--y-axis-label').type('DVORNIK')
        cy.getByTestID('input--Y-axis-pref').type('над')
        cy.getByTestID('input--Y-axis-suff').type('под')
        cy.getByTestID('radio-button--custom').click()
        cy.getByTestID('input--min-val').type('-10{enter}')
        cy.getByTestID('input--max-val').type('110{enter}')

        cy.getByTestID('save-cell--button').click()

        cy.wait(500) //update of cell can take a short moment

        cy.getByTestID('giraffe-layer--line').then(jqCanvas1 => {
          sumFinal = canvasCheckSum(jqCanvas1.get(0) as HTMLCanvasElement)
          expect(sumInit).not.equals(sumFinal)
        })
      })
    })
  })

  describe('Graph with single stat Visualization', () => {
    it('can switch view to graph with stat', () => {
      let sumGraphInit: number = 0
      let sumGraphFinal: number = 0
      let sumAxesInit: number = 0
      let sumAxesFinal: number = 0

      //      cy.wait(500) //do not abort load in beforeEach - leads to undefined objects
      cy.getByTestID('giraffe-layer--line').then(jqcanvas => {
        sumGraphInit = canvasCheckSum(jqcanvas.get(0) as HTMLCanvasElement)
        expect(jqcanvas).to.be.visible
        cy.getByTestID('giraffe-axes').then(jqaxcanv => {
          sumAxesInit = canvasCheckSum(jqaxcanv.get(0) as HTMLCanvasElement)
          expect(jqaxcanv).to.be.visible
        })
      })

      //check initial state
      cy.getByTestID('text--single-stat').should('not.exist')

      cy.getByTestID('context-menu')
        .eq(0)
        .click()
      cy.getByTestID('context-menu-item')
        .eq(0)
        .click()
      cy.getByTestID('button--vis-opts').click()
      cy.contains('Graph').click()
      cy.contains('Graph + Single Stat').click()

      //check base default value
      cy.getByTestID('text--single-stat').should('contain', '6.88')
      cy.getByTestID('text--single-stat').should(
        'have.css',
        'fill',
        'rgb(0, 201, 255)'
      )

      //check prefix
      cy.getByTestID('input--prefix').type('перед')
      cy.getByTestID('text--single-stat').should('contain', 'перед6.88')
      cy.getByTestID('input--prefix').clear()
      cy.getByTestID('text--single-stat').should('contain', '6.88')

      //check suffix
      cy.getByTestID('input--suffix').type('после')
      cy.getByTestID('text--single-stat').should('contain', '6.88после')
      cy.getByTestID('input--suffix').clear()
      cy.getByTestID('text--single-stat').should('contain', '6.88')

      //check decimal places
      cy.getByTestID('input-text--decimal').clear()
      cy.getByTestID('input-text--decimal').type('5{enter}')
      cy.getByTestID('text--single-stat').should('contain', '6.88482')
      cy.getByTestID('radio-default').click()
      cy.getByTestID('text--single-stat').should('contain', '6.88')
      cy.getByTestID('radio-custom').click()
      cy.getByTestID('text--single-stat').should('contain', '6.88')
      cy.getByTestID('input-text--decimal').should('have.value', '')
      //check max 10 constraint
      cy.getByTestID('input-text--decimal').type('15{enter}')
      cy.getByTestID('input-text--decimal').should('have.value', '10')
      cy.getByTestID('text--single-stat').should('contain', '6.8848196084')
      cy.getByTestID('input-text--decimal').clear()
      cy.getByTestID('input-text--decimal').type('3{enter}') //check in DBrd after save

      //change base color
      cy.getByTestID('dropdown--color').click()
      cy.getByTestID('dropdown--color').click() //todo remove after fix of issue 14056
      cy.getByTestID('dropdown--item comet').click()
      cy.getByTestID('text--single-stat').should(
        'have.css',
        'fill',
        'rgb(147, 148, 255)'
      )

      //extra threshold --
      cy.getByTestID('button-add-threshold').click()
      cy.getByTestID('input-text--threshold')
        .invoke('val')
        .then(val => {
          expect(val).is.greaterThan(0)
          expect(val).is.lessThan(100)
        })
      cy.getByTestID('input-text--threshold').clear()
      cy.getByTestID('input-text--threshold').type('5.0')
      cy.getByTestID('input-text--threshold').blur()
      //cy.getByTestID('input-text--threshold').type('{enter}')

      cy.getByTestID('dropdown--color')
        .eq(1)
        .click()
      cy.getByTestID('dropdown--item tiger').click()
      cy.getByTestID('text--single-stat').should(
        'have.css',
        'fill',
        'rgb(244, 141, 56)'
      )

      cy.getByTestID('input--prefix').type('α-')
      cy.getByTestID('input--suffix').type('-ω')

      //todo finish threshold input once issue 14057 is resolved
      /*
          cy.getByTestID('input-text--threshold').clear()
          cy.getByTestID('input-text--threshold').type('20.1{enter}')
          cy.getByTestID('text--single-stat').should('have.css', 'fill', 'rgb(147, 148, 255)')
          */

      //Now check in Dashboard
      cy.getByTestID('save-cell--button').click()
      cy.getByTestID('text--single-stat').should('contain', 'α-6.885-ω')
      cy.getByTestID('text--single-stat').should(
        'have.css',
        'fill',
        'rgb(244, 141, 56)'
      )
      cy.getByTestID('giraffe-layer--line').then(jqcanvas => {
        sumGraphFinal = canvasCheckSum(jqcanvas.get(0) as HTMLCanvasElement)
        expect(jqcanvas).to.be.visible
        expect(sumGraphInit).equals(sumGraphFinal) //nothing concerning the graph should have changed
        cy.getByTestID('giraffe-axes').then(jqaxcanv => {
          sumAxesFinal = canvasCheckSum(jqaxcanv.get(0) as HTMLCanvasElement)
          expect(jqaxcanv).to.be.visible
          expect(sumAxesInit).equals(sumAxesFinal) //nothing concerning axes should have changed
        })
      })
    })
  })

  //todo - add other visualizations e.g. Histogram, Table, etc.
})
