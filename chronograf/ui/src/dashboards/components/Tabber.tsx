import React, {SFC, MouseEvent} from 'react'
import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'

interface TabberProps {
  labelText?: string
  children: JSX.Element[]
  tipID?: string
  tipContent?: string
}

export const Tabber: SFC<TabberProps> = ({
  children,
  tipID = '',
  labelText = '',
  tipContent = '',
}) => (
  <div className="form-group col-md-6">
    <label>
      {labelText}
      {!!tipID && <QuestionMarkTooltip tipID={tipID} tipContent={tipContent} />}
    </label>
    <ul className="nav nav-tablist nav-tablist-sm">{children}</ul>
  </div>
)

interface TabProps {
  onClickTab: (e: MouseEvent<HTMLLIElement>) => void
  isActive: boolean
  text: string
}

export const Tab: SFC<TabProps> = ({isActive, onClickTab, text}) => (
  <li className={isActive ? 'active' : ''} onClick={onClickTab}>
    {text}
  </li>
)
