import React, {PureComponent, Fragment} from 'react'
import _ from 'lodash'

import {FluxContext} from 'src/flux/containers/FluxPage'
import FuncSelector from 'src/flux/components/FuncSelector'
import FuncNode from 'src/flux/components/FuncNode'
import YieldFuncNode from 'src/flux/components/YieldFuncNode'

import {Func} from 'src/types/flux'

interface Props {
  funcNames: any[]
  bodyID: string
  funcs: Func[]
  declarationID?: string
  declarationsFromBody: string[]
  isLastBody: boolean
  onDeleteBody: (bodyID: string) => void
}

interface YieldToggles {
  [x: number]: boolean
}

interface State {
  nonYieldableIndexesToggled: YieldToggles
}

// an Expression is a group of one or more functions
class ExpressionNode extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      nonYieldableIndexesToggled: this.nonYieldableNodesFromScript,
    }
  }

  public componentDidUpdate(prevProps) {
    const {funcs: prevFuncs} = prevProps
    const {funcs} = this.props

    if (!_.isEqual(prevFuncs, funcs)) {
      this.setState({
        nonYieldableIndexesToggled: this.nonYieldableNodesFromScript,
      })
    }
  }

  public render() {
    const {
      declarationID,
      bodyID,
      funcNames,
      funcs,
      declarationsFromBody,
      onDeleteBody,
    } = this.props

    const {nonYieldableIndexesToggled} = this.state

    return (
      <FluxContext.Consumer>
        {({
          onDeleteFuncNode,
          onAddNode,
          onChangeArg,
          onGenerateScript,
          onToggleYield,
          service,
          data,
          scriptUpToYield,
        }) => {
          let isAfterRange = false
          let isAfterFilter = false

          return (
            <>
              {funcs.map((func, i) => {
                if (func.name === 'range') {
                  isAfterRange = true
                }

                if (func.name === 'filter') {
                  isAfterFilter = true
                }

                if (func.name === 'yield') {
                  const script = scriptUpToYield(bodyID, declarationID, i, true)

                  return (
                    <YieldFuncNode
                      index={i}
                      key={i}
                      func={func}
                      data={data}
                      script={script}
                      bodyID={bodyID}
                      service={service}
                      declarationID={declarationID}
                    />
                  )
                }

                const funcNode = (
                  <FuncNode
                    key={i}
                    index={i}
                    func={func}
                    funcs={funcs}
                    bodyID={bodyID}
                    service={service}
                    onChangeArg={onChangeArg}
                    onDelete={onDeleteFuncNode}
                    onToggleYield={onToggleYield}
                    isYieldable={isAfterFilter && isAfterRange}
                    isYielding={this.isBeforeYielding(i)}
                    isYieldedInScript={this.isYieldNodeIndex(i + 1)}
                    declarationID={declarationID}
                    onGenerateScript={onGenerateScript}
                    declarationsFromBody={declarationsFromBody}
                    onToggleYieldWithLast={this.handleToggleYieldWithLast}
                    onDeleteBody={onDeleteBody}
                  />
                )

                if (
                  nonYieldableIndexesToggled[i] &&
                  !this.isYieldNodeIndex(i + 1)
                ) {
                  const script = scriptUpToYield(
                    bodyID,
                    declarationID,
                    i,
                    false
                  )

                  return (
                    <Fragment key={`${i}-notInScript`}>
                      {funcNode}
                      <YieldFuncNode
                        index={i}
                        func={func}
                        data={data}
                        script={script}
                        bodyID={bodyID}
                        service={service}
                        declarationID={declarationID}
                      />
                    </Fragment>
                  )
                }
                return funcNode
              })}
              <FuncSelector
                bodyID={bodyID}
                funcs={funcNames}
                onAddNode={onAddNode}
                declarationID={declarationID}
              />
            </>
          )
        }}
      </FluxContext.Consumer>
    )
  }

  private isBeforeYielding(funcIndex: number): boolean {
    const {nonYieldableIndexesToggled} = this.state
    const beforeToggledLastYield = !!nonYieldableIndexesToggled[funcIndex]

    if (beforeToggledLastYield) {
      return true
    }

    return this.isYieldNodeIndex(funcIndex + 1)
  }

  private isYieldNodeIndex(funcIndex) {
    const {funcs} = this.props
    const nextFunc = _.get(funcs, `${funcIndex}`, null)

    if (nextFunc && nextFunc.name === 'yield') {
      return true
    }

    return false
  }

  private get nonYieldableNodesFromScript(): YieldToggles {
    const {funcs} = this.props
    let isBeforeFilter = true
    let isBeforeRange = true

    return _.reduce(
      funcs,
      (acc: YieldToggles, f, index) => {
        if (f.name === 'range') {
          isBeforeRange = false
        }

        if (f.name === 'filter') {
          isBeforeFilter = false
        }

        if (isBeforeFilter || isBeforeRange) {
          if (this.isYieldNodeIndex(index + 1)) {
            return {...acc, [index]: true}
          }
        }

        return acc
      },
      {}
    )
  }

  // if funcNode is not yieldable, add last before yield()
  private handleToggleYieldWithLast = (funcNodeIndex: number) => {
    this.setState(({nonYieldableIndexesToggled}) => {
      const isFuncYieldToggled = !!nonYieldableIndexesToggled[funcNodeIndex]

      return {
        nonYieldableIndexesToggled: {
          ...nonYieldableIndexesToggled,
          [funcNodeIndex]: !isFuncYieldToggled,
        },
      }
    })
  }
}

export default ExpressionNode
