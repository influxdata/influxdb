import {Organization} from '@influxdata/influx'

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
                  `${orgs}/${id}/dashboards/${dbId}/cells/${cellId}/edit?lower=now%28%29%20-%205m`
                )
                cy.getByTestID('button--vis-opts').click()
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
      //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
      //cy.getByTestID('vis-layer--line')..then((jqcanvas1) => {
      cy.get('.time-machine--view .vis-layer').then(jqcanvas1 => {
        let sum1 = canvasCheckSum(jqcanvas1.get(0) as HTMLCanvasElement)

        cy.contains('Linear').click()
        cy.contains('Smooth').click()

        //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
        //cy.getByTestID('vis-layer--line').then((jqcanvas2) => {
        cy.get('.time-machine--view .vis-layer').then(jqcanvas2 => {
          let sum2 = canvasCheckSum(jqcanvas2.get(0) as HTMLCanvasElement)

          expect(sum2).not.equals(sum1)
        })
      })
    })

    it('can change the line interpolation to Step', () => {
      //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
      //cy.getByTestID('vis-layer--line')..then((jqcanvas1) => {
      cy.get('.time-machine--view .vis-layer').then(jqcanvas1 => {
        let sum1 = canvasCheckSum(jqcanvas1.get(0) as HTMLCanvasElement)

        cy.contains('Linear').click()
        cy.contains('Step').click()

        //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
        //cy.getByTestID('vis-layer--line').then((jqcanvas2) => {
        cy.get('.time-machine--view .vis-layer').then(jqcanvas2 => {
          let sum2 = canvasCheckSum(jqcanvas2.get(0) as HTMLCanvasElement)

          expect(sum2).not.equals(sum1)
        })
      })
    })

    it('can change the line colors to Delorean', () => {
      //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
      //cy.getByTestID('vis-layer--line')..then((jqcanvas1) => {
      cy.get('.time-machine--view .vis-layer').then(jqcanvas1 => {
        let sum1 = canvasCheckSum(jqcanvas1.get(0) as HTMLCanvasElement)

        cy.contains('Nineteen Eighty Four').click()
        cy.contains('Delorean').click()

        //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
        //cy.getByTestID('vis-layer--line').then((jqcanvas2) => {
        cy.get('.time-machine--view .vis-layer').then(jqcanvas2 => {
          let sum2 = canvasCheckSum(jqcanvas2.get(0) as HTMLCanvasElement)

          expect(sum2).not.equals(sum1)
        })
      })
    })

    it('can modify the Y axis label', () => {
      //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
      //cy.getByTestID('vis-layer--line')..then((jqcanvas1) => {
      cy.get('.vis-axes').then(jqcanvas1 => {
        let sum1 = canvasCheckSum(jqcanvas1.get(1) as HTMLCanvasElement)

        // N.B. updates contents of canvas .vis-axes
        cy.getByTestID('input--y-axis-label').type('DVORNIK')

        //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
        //cy.getByTestID('vis-layer--line').then((jqcanvas2) => {
        cy.get('.vis-axes').then(jqcanvas2 => {
          let sum2 = canvasCheckSum(jqcanvas2.get(1) as HTMLCanvasElement)

          expect(sum2).not.equals(sum1)
        })
      })
    })

    it('can modify the Y tick prefix', () => {
      //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
      //cy.getByTestID('vis-layer--line')..then((jqcanvas1) => {
      cy.get('.vis-axes').then(jqcanvas1 => {
        let sum1 = canvasCheckSum(jqcanvas1.get(1) as HTMLCanvasElement)

        // N.B. updates contents of canvas .vis-axes
        cy.getByTestID('input--Y-axis-pref').type('над')

        //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
        //cy.getByTestID('vis-layer--line').then((jqcanvas2) => {
        cy.get('.vis-axes').then(jqcanvas2 => {
          let sum2 = canvasCheckSum(jqcanvas2.get(1) as HTMLCanvasElement)

          expect(sum2).not.equals(sum1)
        })
      })
    })

    it('can modify the T tick suffix', () => {
      //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
      //cy.getByTestID('vis-layer--line')..then((jqcanvas1) => {
      cy.get('.vis-axes').then(jqcanvas1 => {
        let sum1 = canvasCheckSum(jqcanvas1.get(1) as HTMLCanvasElement)

        // N.B. updates contents of canvas .vis-axes
        cy.getByTestID('input--Y-axis-suff').type('под')

        //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
        //cy.getByTestID('vis-layer--line').then((jqcanvas2) => {
        cy.get('.vis-axes').then(jqcanvas2 => {
          let sum2 = canvasCheckSum(jqcanvas2.get(1) as HTMLCanvasElement)

          expect(sum2).not.equals(sum1)
        })
      })
    })

    it('can set a custom Y axis domain', () => {
      //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
      //cy.getByTestID('vis-layer--line')..then((jqcanvas1) => {
      cy.get('.vis-axes').then(jqcanvas1 => {
        let sum1 = canvasCheckSum(jqcanvas1.get(1) as HTMLCanvasElement)

        // N.B. updates contents of canvas .vis-axes
        cy.getByTestID('radio-button--custom').click()
        cy.getByTestID('input--min-val').type('-10{enter}')
        cy.getByTestID('form--element-error').should(
          'have.text',
          'Must supply a valid maximum value'
        )
        cy.getByTestID('input--max-val').type('110{enter}')

        //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
        //cy.getByTestID('vis-layer--line').then((jqcanvas2) => {
        cy.get('.vis-axes').then(jqcanvas2 => {
          let sum2 = canvasCheckSum(jqcanvas2.get(1) as HTMLCanvasElement)

          expect(sum2).not.equals(sum1)
        })
      })
    })

    //comprehensive test of above - begin in dashboard view, modify everything, save and check cell in dashboard
    it('can modify the dashboard view of the cell', () => {
      let sumInit: number
      let sumFinal: number

      cy.get<Organization>('@org').then(({id}) => {
        cy.fixture('routes').then(({orgs}) => {
          cy.visit(`${orgs}/${id}/dashboards/${dbId}?lower=now%28%29%20-%206h`)
          cy.contains('TEST CELL').should('be.visible')
          //todo testid from vis
          cy.get('.vis-layer.line').should('be.visible')

          cy.get('.vis-layer.line').then(jqCanvas1 => {
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

            cy.get('.vis-layer.line').then(jqCanvas1 => {
              sumFinal = canvasCheckSum(jqCanvas1.get(0) as HTMLCanvasElement)
              expect(sumInit).not.equals(sumFinal)
            })
          })
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

      cy.wait(500) //do not abort load in beforeEach - leads to undefined objects
      cy.get<Organization>('@org').then(({id}) => {
        cy.fixture('routes').then(({orgs}) => {
          cy.visit(`${orgs}/${id}/dashboards/${dbId}?lower=now%28%29%20-%206h`)
          cy.get('.vis-layer.line').then(jqcanvas => {
            sumGraphInit = canvasCheckSum(jqcanvas.get(0) as HTMLCanvasElement)
            expect(jqcanvas).to.be.visible
            cy.get('.vis-axes').then(jqaxcanv => {
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
          cy.get('.vis-layer.line').then(jqcanvas => {
            sumGraphFinal = canvasCheckSum(jqcanvas.get(0) as HTMLCanvasElement)
            expect(jqcanvas).to.be.visible
            expect(sumGraphInit).equals(sumGraphFinal) //nothing concerning the graph should have changed
            cy.get('.vis-axes').then(jqaxcanv => {
              sumAxesFinal = canvasCheckSum(jqaxcanv.get(
                0
              ) as HTMLCanvasElement)
              expect(jqaxcanv).to.be.visible
              expect(sumAxesInit).equals(sumAxesFinal) //nothing concerning axes should have changed
            })
          })
        })
      })
    })
  })

  //todo - add other visualizations e.g. Histogram, Table, etc.
})
