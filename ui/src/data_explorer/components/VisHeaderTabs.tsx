import React, {SFC} from 'react'
import VisHeaderTab, {
  OnToggleView,
} from 'src/data_explorer/components/VisHeaderTab'

interface Props {
  views: string[]
  view: string
  currentView: string
  onToggleView: OnToggleView
}

const VisHeaderTabs: SFC<Props> = ({views, currentView, onToggleView}) => {
  return (
    <ul className="nav nav-tablist nav-tablist-sm">
      {views.map(v => (
        <VisHeaderTab
          key={v}
          view={v}
          currentView={currentView}
          onToggleView={onToggleView}
        />
      ))}
    </ul>
  )
}

export default VisHeaderTabs
