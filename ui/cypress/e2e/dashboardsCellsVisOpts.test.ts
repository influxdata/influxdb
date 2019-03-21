// Covers creating cells and managing view options
describe('cells veo', () => {
  let nowNano = new Date().getTime() * 1000000
  let intervalNano: number = 600 * 1000 * 1000000 //10 min in nanosecs
  let lines: string[] = []
  let recCount = 256
  let startTime = nowNano - recCount * intervalNano

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
      //cy.wrap(body.org).as('org')
      cy.writeData(lines)

      let dbId: string
      let cellId: string

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
        cy.getByTestID('input-y-axis-prefix').type('над')

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
        cy.getByTestID('input-y-axis-suffix').type('под')

        //todo - activate testID instead of classes once https://github.com/influxdata/vis/pull/69 is merged and released
        //cy.getByTestID('vis-layer--line').then((jqcanvas2) => {
        cy.get('.vis-axes').then(jqcanvas2 => {
          let sum2 = canvasCheckSum(jqcanvas2.get(1) as HTMLCanvasElement)

          expect(sum2).not.equals(sum1)
        })
      })
    })

    it.only('can set a custom Y axis domain', () => {
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

    //comprehensive test of above - begin in dashboard view, modify everything, save and check everything
    it.skip('can modify the dashboard view of the cell', () => {})
  })
})
