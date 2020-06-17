// Libraries
import React, {
  FC,
  KeyboardEvent,
  ChangeEvent,
  MouseEvent,
  useState,
} from 'react'
import classnames from 'classnames'

// Components
import {Input, InputRef, Icon, IconFont, Page} from '@influxdata/clockface'
import {ClickOutside} from 'src/shared/components/ClickOutside'

interface Props {
  onRename: (name: string) => void
  onClickOutside?: (e: MouseEvent<HTMLElement>) => void
  name: string
  placeholder: string
  maxLength: number
}

const RenamablePageTitle: FC<Props> = ({
  onRename,
  onClickOutside,
  name,
  placeholder,
  maxLength,
}) => {
  const [isEditing, setEditingState] = useState<boolean>(false)
  const [workingName, setWorkingName] = useState<string>(name)

  const handleStartEditing = (): void => {
    setEditingState(true)
  }

  const handleStopEditing = (e: MouseEvent<any>): void => {
    onRename(workingName)

    if (onClickOutside) {
      onClickOutside(e)
    }

    setEditingState(false)
  }

  const handleInputChange = (e: ChangeEvent<InputRef>): void => {
    setWorkingName(e.target.value)
  }

  const handleKeyDown = (e: KeyboardEvent<InputRef>): void => {
    if (e.key === 'Enter') {
      onRename(workingName)
      setEditingState(false)
    }

    if (e.key === 'Escape') {
      setEditingState(false)
      setWorkingName(name)
    }
  }

  const handleInputFocus = (e: ChangeEvent<InputRef>): void => {
    e.currentTarget.select()
  }

  const nameIsUntitled = name === placeholder || name === ''

  const renamablePageTitleClass = classnames('renamable-page-title', {
    untitled: nameIsUntitled,
  })

  if (isEditing) {
    return (
      <ClickOutside onClickOutside={handleStopEditing}>
        <div
          className={renamablePageTitleClass}
          data-testid="renamable-page-title"
        >
          <Input
            maxLength={maxLength}
            autoFocus={true}
            spellCheck={false}
            placeholder={placeholder}
            onFocus={handleInputFocus}
            onChange={handleInputChange}
            onKeyDown={handleKeyDown}
            className="renamable-page-title--input"
            value={workingName}
            testID="renamable-page-title--input"
          />
        </div>
      </ClickOutside>
    )
  }

  return (
    <div className={renamablePageTitleClass} onClick={handleStartEditing}>
      <Page.Title title={workingName || placeholder} />
      <Icon glyph={IconFont.Pencil} className="renamable-page-title--icon" />
    </div>
  )
}

export default RenamablePageTitle
