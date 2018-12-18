// Libraries
import React, {PureComponent, SFC} from 'react'
import uuid from 'uuid'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Components
import {IndexList, ComponentColor} from 'src/clockface'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'
import Context from 'src/clockface/components/context_menu/Context'

// Types
import {IconFont} from 'src/clockface/types'
import {TelegrafPluginName} from 'src/types/v2/dataLoaders'

// Actions
import {setConfigArrayValue} from 'src/onboarding/actions/dataLoaders'

interface Item {
  text?: string
  name?: string
}

interface RowsProps {
  tags: Item[]
  confirmText?: string
  onDeleteTag?: (item: Item) => void
  onSetConfigArrayValue: typeof setConfigArrayValue
  fieldName: string
  telegrafPluginName: TelegrafPluginName
}

const Rows: SFC<RowsProps> = ({
  tags,
  onDeleteTag,
  onSetConfigArrayValue,
  fieldName,
  telegrafPluginName,
}) => {
  return (
    <div className="input-tag-list">
      {tags.map(item => {
        return (
          <Row
            index={tags.indexOf(item)}
            key={uuid.v4()}
            item={item}
            onDelete={onDeleteTag}
            onSetConfigArrayValue={onSetConfigArrayValue}
            fieldName={fieldName}
            telegrafPluginName={telegrafPluginName}
          />
        )
      })}
    </div>
  )
}

interface RowProps {
  confirmText?: string
  item: Item
  onDelete: (item: Item) => void
  onSetConfigArrayValue: typeof setConfigArrayValue
  fieldName: string
  telegrafPluginName: TelegrafPluginName
  index: number
}

@ErrorHandling
class Row extends PureComponent<RowProps> {
  public static defaultProps: Partial<RowProps> = {
    confirmText: 'Delete',
  }

  public render() {
    const {item} = this.props
    return (
      <IndexList>
        <IndexList.Body emptyState={<div />} columnCount={2}>
          <IndexList.Row key={uuid.v4()} disabled={false}>
            <IndexList.Cell>
              <InputClickToEdit
                value={item.text}
                onKeyDown={this.handleKeyDown}
              />
            </IndexList.Cell>
            <IndexList.Cell>
              <Context.Menu icon={IconFont.Trash} color={ComponentColor.Danger}>
                <Context.Item
                  label="Delete"
                  action={this.handleClickDelete(item)}
                />
              </Context.Menu>
            </IndexList.Cell>
          </IndexList.Row>
        </IndexList.Body>
      </IndexList>
    )
  }
  private handleClickDelete = item => () => {
    this.props.onDelete(item)
  }
  private handleKeyDown = (value: string) => {
    const {
      telegrafPluginName,
      fieldName,
      onSetConfigArrayValue,
      index,
    } = this.props
    onSetConfigArrayValue(telegrafPluginName, fieldName, index, value)
  }
}

export default Rows
