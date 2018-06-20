import React, {PureComponent, ReactElement, SFC} from 'react'

import classnames from 'classnames'

interface TabProps {
  children: JSX.Element[] | JSX.Element | string
  onClick?: () => void
  isDisabled?: boolean
  isActive?: boolean
  isKapacitorTab?: boolean
  isConfigured?: boolean
}

export const Tab: SFC<TabProps> = ({
  children,
  onClick,
  isDisabled,
  isActive,
  isKapacitorTab,
  isConfigured,
}) => {
  if (isKapacitorTab) {
    return (
      <li
        className={classnames({active: isActive})}
        onClick={isDisabled ? null : onClick}
      >
        {children}
      </li>
    )
  }

  return (
    <div
      className={classnames('btn tab', {
        active: isActive,
        configured: isConfigured,
      })}
      onClick={isDisabled ? null : onClick}
    >
      {children}
    </div>
  )
}

interface TabListProps {
  children: JSX.Element[] | JSX.Element
  activeIndex?: number
  onActivate?: (index: number) => void
  isKapacitorTabs?: string
  customClass?: string
}

export const TabList: SFC<TabListProps> = ({
  children,
  activeIndex,
  onActivate,
  isKapacitorTabs,
  customClass,
}) => {
  const kids = React.Children.map(
    children,
    (child: ReactElement<any>, index) => {
      return React.cloneElement(child, {
        isActive: index === activeIndex,
        onClick: () => onActivate(index),
      })
    }
  )

  if (isKapacitorTabs === 'true') {
    return (
      <div className="rule-section--row rule-section--row-first rule-section--row-last">
        <p>Choose One:</p>
        <div className="nav nav-tablist nav-tablist-sm nav-tablist-malachite">
          {kids}
        </div>
      </div>
    )
  }

  if (customClass) {
    return (
      <div className={customClass}>
        <div className="btn-group btn-group-lg tab-group">{kids}</div>
      </div>
    )
  }

  return <div className="btn-group btn-group-lg tab-group">{kids}</div>
}

TabList.defaultProps = {
  isKapacitorTabs: '',
}

interface TabPanelsProps {
  children: JSX.Element[] | JSX.Element
  activeIndex?: number
  customClass?: string
}

export const TabPanels: SFC<TabPanelsProps> = ({
  children,
  activeIndex,
  customClass,
}) => (
  // if only 1 child, children array index lookup will fail
  <div className={customClass}>{children[activeIndex]}</div>
)

interface TabPanelProps {
  children: JSX.Element[] | JSX.Element
}

export const TabPanel: SFC<TabPanelProps> = ({children}) => (
  <div>{children}</div>
)

interface TabsProps {
  children: JSX.Element[] | JSX.Element
  onSelect?: (activeIndex: number) => void
  tabContentsClass?: string
  tabsClass?: string
  initialIndex?: number
}

interface TabsState {
  activeIndex: number
}

export class Tabs extends PureComponent<TabsProps, TabsState> {
  public static defaultProps: Partial<TabsProps> = {
    onSelect: () => {},
    tabContentsClass: '',
  }

  constructor(props) {
    super(props)

    // initialIndex allows the user to enable a Tab and TabPanel
    // other than 0 on initial render.
    this.state = {
      activeIndex: this.props.initialIndex || 0,
    }
  }

  public handleActivateTab = activeIndex => {
    this.setState({activeIndex})
    this.props.onSelect(activeIndex)
  }

  public render() {
    const {children, tabContentsClass} = this.props

    const kids = React.Children.map(children, (child: ReactElement<any>) => {
      if (child && child.type === TabPanels) {
        return React.cloneElement(child, {
          activeIndex: this.state.activeIndex,
        })
      }

      if (child && child.type === TabList) {
        return React.cloneElement(child, {
          activeIndex: this.state.activeIndex,
          onActivate: this.handleActivateTab,
        })
      }

      return child
    })

    return <div className={tabContentsClass}>{kids}</div>
  }
}
