// Libraries
import React, {FunctionComponent, useState} from 'react'
import classnames from 'classnames'

// Components
import {SquareButton, IconFont, ClickOutside} from '@influxdata/clockface'

export enum QBCardType {
  Filter = 'filter',
  Func = 'func',
}

interface Props {
  onSelectCard: (type: QBCardType) => void
}

const AddCardButton: FunctionComponent<Props> = ({onSelectCard}) => {
  const [isMenuVisible, setMenuVisibility] = useState<boolean>(false)

  const menuClass = classnames('query-builder--add-card-menu', {
    visible: isMenuVisible,
  })

  const onSelect = (type: QBCardType) => () => {
    onSelectCard(type)
    setMenuVisibility(false)
  }

  return (
    <div className="query-builder--add-card-button">
      <SquareButton
        onClick={() => setMenuVisibility(!isMenuVisible)}
        icon={IconFont.Plus}
      />
      <ClickOutside onClickOutside={() => setMenuVisibility(false)}>
        <div className={menuClass}>
          <div
            className="query-builder--add-card-item"
            onClick={onSelect(QBCardType.Filter)}
          >
            Filter
          </div>
          <div
            className="query-builder--add-card-item"
            onClick={onSelect(QBCardType.Func)}
          >
            Aggregate
          </div>
        </div>
      </ClickOutside>
    </div>
  )
}

export default AddCardButton
