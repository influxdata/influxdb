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
}

interface State {
  nonYieldableIndexesToggled: {
    [x: number]: boolean
  }
  isImplicitYieldToggled: boolean
}

// an Expression is a group of one or more functions
class ExpressionNode extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      nonYieldableIndexesToggled: {},
      isImplicitYieldToggled: this.isImplicitYieldToggled,
    }
  }

  public render() {
    const {
      declarationID,
      bodyID,
      funcNames,
      funcs,
      declarationsFromBody,
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
                    bodyID={bodyID}
                    service={service}
                    onChangeArg={onChangeArg}
                    onDelete={onDeleteFuncNode}
                    onToggleYield={onToggleYield}
                    isYieldable={isAfterFilter && isAfterRange}
                    isYielding={this.isBeforeFuncYield(i)}
                    declarationID={declarationID}
                    onGenerateScript={onGenerateScript}
                    declarationsFromBody={declarationsFromBody}
                    onToggleYieldWithLast={this.handleToggleYieldWithLast}
                  />
                )

                if (nonYieldableIndexesToggled[i]) {
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
                } else if (this.isEndOfScript(i)) {
                  const script = scriptUpToYield(bodyID, declarationID, i, true)

                  return (
                    <Fragment key={`${i}-notInScript`}>
                      <FuncNode
                        key={i}
                        index={i}
                        func={func}
                        bodyID={bodyID}
                        service={service}
                        onChangeArg={onChangeArg}
                        onDelete={onDeleteFuncNode}
                        onToggleYield={this.handleHideImplicitYield}
                        isYieldable={isAfterFilter && isAfterRange}
                        isYielding={this.isBeforeFuncYield(i)}
                        declarationID={declarationID}
                        onGenerateScript={onGenerateScript}
                        declarationsFromBody={declarationsFromBody}
                        onToggleYieldWithLast={this.handleToggleYieldWithLast}
                      />
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
                } else {
                  return funcNode
                }
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

  private isBeforeFuncYield(funcIndex: number): boolean {
    const {funcs, isLastBody} = this.props
    const {isImplicitYieldToggled} = this.state

    if (
      funcIndex === funcs.length - 1 &&
      isLastBody &&
      isImplicitYieldToggled
    ) {
      return true
    }

    if (funcIndex === funcs.length - 1) {
      return false
    }

    const nextFunc = funcs[funcIndex + 1]

    if (nextFunc.name === 'yield') {
      return true
    }

    return false
  }

  private get isImplicitYieldToggled(): boolean {
    const {isLastBody} = this.props

    return isLastBody && this.isLastFuncYield
  }

  private get isLastFuncYield(): boolean {
    const {funcs} = this.props

    return _.get(funcs, `${funcs.length - 1}.name`) !== 'yield'
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

  private handleHideImplicitYield = () => {
    this.setState(() => ({
      isImplicitYieldToggled: false,
    }))
  }

  private isEndOfScript(index: number): boolean {
    const {isLastBody, funcs} = this.props
    const {isImplicitYieldToggled} = this.state
    const isLastScriptFunc = isLastBody && index === funcs.length - 1

    return isLastScriptFunc && isImplicitYieldToggled
  }
}

export default ExpressionNode
