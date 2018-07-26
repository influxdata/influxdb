import React, {PureComponent, Fragment} from 'react'

import {FluxContext} from 'src/flux/containers/FluxPage'
import FuncSelector from 'src/flux/components/FuncSelector'
import FuncNode from 'src/flux/components/FuncNode'
import YieldFuncNode from 'src/flux/components/YieldFuncNode'
import {getDeep} from 'src/utils/wrappers'

import {Func, Context} from 'src/types/flux'

interface Props {
  funcNames: any[]
  bodyID: string
  funcs: Func[]
  declarationID?: string
  declarationsFromBody: string[]
  isLastBody: boolean
  onDeleteBody: (bodyID: string) => void
}

interface State {
  nonYieldableIndexesToggled: {
    [x: number]: boolean
  }
}

// an Expression is a group of one or more functions
class ExpressionNode extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      nonYieldableIndexesToggled: {},
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
          source,
          data,
          scriptUpToYield,
        }: Context) => {
          let isAfterRange = false
          let isAfterFilter = false

          return (
            <>
              {funcs.map((func, i) => {
                if (func.name === 'yield') {
                  return null
                }

                if (func.name === 'range') {
                  isAfterRange = true
                }

                if (func.name === 'filter') {
                  isAfterFilter = true
                }
                const isYieldable = isAfterFilter && isAfterRange

                const funcNode = (
                  <FuncNode
                    key={i}
                    index={i}
                    func={func}
                    funcs={funcs}
                    bodyID={bodyID}
                    source={source}
                    onChangeArg={onChangeArg}
                    onDelete={onDeleteFuncNode}
                    onToggleYield={onToggleYield}
                    isYieldable={isYieldable}
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
                  nonYieldableIndexesToggled[i] ||
                  this.isYieldNodeIndex(i + 1)
                ) {
                  const script: string = scriptUpToYield(
                    bodyID,
                    declarationID,
                    i,
                    isYieldable
                  )

                  let yieldFunc = func

                  if (this.isYieldNodeIndex(i + 1)) {
                    yieldFunc = funcs[i + 1]
                  }

                  return (
                    <Fragment key={`${i}-notInScript`}>
                      {funcNode}
                      <YieldFuncNode
                        index={i}
                        func={yieldFunc}
                        data={data}
                        script={script}
                        bodyID={bodyID}
                        source={source}
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

  private isYieldNodeIndex(funcIndex: number): boolean {
    const {funcs} = this.props
    const funcName = getDeep<string>(funcs, `${funcIndex}.name`, '')

    return funcName === 'yield'
  }

  // if funcNode is not yieldable, add last before yield()
  private handleToggleYieldWithLast = (funcNodeIndex: number): void => {
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
