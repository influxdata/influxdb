import React, {PureComponent, MouseEvent} from 'react'
import _ from 'lodash'

import {
  OnChangeArg,
  Func,
  FilterClause,
  FilterTagCondition,
  FilterNode,
} from 'src/types/flux'
import {argTypes} from 'src/flux/constants'

import FuncArgTextArea from 'src/flux/components/FuncArgTextArea'
import FilterTagListItem from 'src/flux/components/FilterTagListItem'
import FancyScrollbar from '../../shared/components/FancyScrollbar'
import {getDeep} from 'src/utils/wrappers'

import {SchemaFilter} from 'src/types'
import {Source} from 'src/types/v2'

interface Props {
  db: string
  source: Source
  tags: string[]
  filter: SchemaFilter[]
  onChangeArg: OnChangeArg
  func: Func
  nodes: FilterNode[]
  bodyID: string
  declarationID: string
  onGenerateScript: () => void
}

type ParsedClause = [FilterClause, boolean]

export default class FilterTagList extends PureComponent<Props> {
  public get clauseIsParseable(): boolean {
    const [, parseable] = this.reduceNodesToClause(this.props.nodes, [])
    return parseable
  }

  public get clause(): FilterClause {
    const [clause] = this.reduceNodesToClause(this.props.nodes, [])
    return clause
  }

  public conditions(key: string, clause?): FilterTagCondition[] {
    clause = clause || this.clause
    return clause[key] || []
  }

  public operator(key: string, clause?): string {
    const conditions = this.conditions(key, clause)
    return getDeep<string>(conditions, '0.operator', '==')
  }

  public addCondition(condition: FilterTagCondition): FilterClause {
    const conditions = this.conditions(condition.key)
    return {
      ...this.clause,
      [condition.key]: [...conditions, condition],
    }
  }

  public removeCondition(condition: FilterTagCondition): FilterClause {
    const conditions = this.conditions(condition.key)
    const newConditions = _.reject(conditions, c => _.isEqual(c, condition))
    return {
      ...this.clause,
      [condition.key]: newConditions,
    }
  }

  public buildFilterString(clause: FilterClause): string {
    const funcBody = Object.entries(clause)
      .filter(([__, conditions]) => conditions.length)
      .map(([key, conditions]) => {
        const joiner = this.operator(key, clause) === '==' ? ' OR ' : ' AND '
        const subClause = conditions
          .map(c => `r.${key} ${c.operator} "${c.value}"`)
          .join(joiner)
        return '(' + subClause + ')'
      })
      .join(' AND ')
    return funcBody ? `(r) => ${funcBody}` : `() => true`
  }

  public handleChangeValue = (
    key: string,
    value: string,
    selected: boolean
  ): void => {
    const condition: FilterTagCondition = {
      key,
      operator: this.operator(key),
      value,
    }
    const clause: FilterClause = selected
      ? this.addCondition(condition)
      : this.removeCondition(condition)
    const filterString: string = this.buildFilterString(clause)
    this.updateFilterString(filterString)
  }

  public handleSetEquality = (key: string, equal: boolean): void => {
    const operator = equal ? '==' : '!='
    const clause: FilterClause = {
      ...this.clause,
      [key]: this.conditions(key).map(c => ({...c, operator})),
    }
    const filterString: string = this.buildFilterString(clause)
    this.updateFilterString(filterString)
  }

  public updateFilterString = (newFilterString: string): void => {
    const {
      func: {id},
      bodyID,
      declarationID,
    } = this.props

    this.props.onChangeArg({
      funcID: id,
      key: 'fn',
      value: newFilterString,
      declarationID,
      bodyID,
      generate: true,
    })
  }

  public render() {
    const {
      db,
      source,
      tags,
      filter,
      bodyID,
      declarationID,
      onChangeArg,
      onGenerateScript,
      func: {id: funcID, args},
    } = this.props
    const {value, key: argKey} = args[0]

    if (!this.clauseIsParseable) {
      return (
        <>
          <p className="flux-filter--helper-text">
            Unable to render expression as a Builder
          </p>
          <FuncArgTextArea
            type={argTypes.STRING}
            value={value}
            argKey={argKey}
            funcID={funcID}
            bodyID={bodyID}
            onChangeArg={onChangeArg}
            declarationID={declarationID}
            onGenerateScript={onGenerateScript}
          />
        </>
      )
    }

    if (tags.length) {
      return (
        <FancyScrollbar className="flux-filter--fancyscroll" maxHeight={600}>
          {tags.map(t => (
            <FilterTagListItem
              key={t}
              db={db}
              tagKey={t}
              conditions={this.conditions(t)}
              operator={this.operator(t)}
              onChangeValue={this.handleChangeValue}
              onSetEquality={this.handleSetEquality}
              source={source}
              filter={filter}
            />
          ))}
        </FancyScrollbar>
      )
    }

    return (
      <div className="flux-schema-tree">
        <div className="flux-schema--item no-hover" onClick={this.handleClick}>
          <div className="no-results">No tag keys found.</div>
        </div>
      </div>
    )
  }

  public reduceNodesToClause(
    nodes,
    conditions: FilterTagCondition[]
  ): ParsedClause {
    if (!nodes.length) {
      return this.constructClause(conditions)
    } else if (this.noConditions(nodes, conditions)) {
      return [{}, true]
    } else if (
      ['OpenParen', 'CloseParen', 'Operator'].includes(nodes[0].type)
    ) {
      return this.skipNode(nodes, conditions)
    } else if (this.conditionExtractable(nodes)) {
      return this.extractCondition(nodes, conditions)
    } else {
      // Unparseable
      return [{}, false]
    }
  }

  private constructClause(conditions: FilterTagCondition[]): ParsedClause {
    const clause = _.groupBy(conditions, condition => condition.key)
    if (this.validateClause(clause)) {
      return [clause, true]
    } else {
      return [{}, false]
    }
  }

  private validateClause(clause) {
    return Object.values(clause).every((conditions: FilterTagCondition[]) =>
      conditions.every(c => conditions[0].operator === c.operator)
    )
  }

  private noConditions(nodes, conditions) {
    return (
      !conditions.length &&
      nodes.length === 1 &&
      nodes[0].type === 'BooleanLiteral' &&
      nodes[0].source === 'true'
    )
  }

  private skipNode([, ...nodes], conditions) {
    return this.reduceNodesToClause(nodes, conditions)
  }

  private conditionExtractable(nodes): boolean {
    return (
      nodes.length >= 3 &&
      nodes[0].type === 'MemberExpression' &&
      nodes[1].type === 'Operator' &&
      this.supportedOperator(nodes[1].source) &&
      nodes[2].type === 'StringLiteral'
    )
  }

  private supportedOperator(operator): boolean {
    return operator === '==' || operator === '!='
  }

  private extractCondition(
    [keyNode, operatorNode, valueNode, ...nodes],
    conditions
  ) {
    const condition: FilterTagCondition = {
      key: keyNode.property.name,
      operator: operatorNode.source,
      value: valueNode.source.replace(/"/g, ''),
    }
    return this.reduceNodesToClause(nodes, [...conditions, condition])
  }

  private handleClick(e: MouseEvent<HTMLDivElement>) {
    e.stopPropagation()
  }
}
