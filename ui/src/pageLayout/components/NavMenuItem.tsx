// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'
import classnames from 'classnames'

// Types
import {IconFont, NavMenuType} from 'src/clockface'

interface PassedProps {
  icon: IconFont
  title: string
  path: string
  children?: JSX.Element | JSX.Element[]
  active: boolean
  className?: string
}

interface DefaultProps {
  type: NavMenuType
  testID: string
}

type Props = PassedProps & Partial<DefaultProps>

class NavMenuItem extends PureComponent<Props> {
  public static defaultProps: DefaultProps = {
    type: NavMenuType.RouterLink,
    testID: 'nav-menu--item',
  }

  public render() {
    const {
      icon,
      title,
      path,
      children,
      active,
      testID,
      type,
      className,
    } = this.props

    if (type === NavMenuType.RouterLink) {
      return (
        <div
          className={classnames('nav--item', {
            active,
            [`${className}`]: className,
          })}
          data-testid={`${testID} ${title}`}
        >
          <Link className="nav--item-icon" to={path}>
            <span className={`icon sidebar--icon ${icon}`} />
          </Link>
          <div className="nav--item-menu">
            <Link className="nav--item-header" to={path}>
              {title}
            </Link>
            {children}
          </div>
        </div>
      )
    }

    return (
      <div
        className={classnames('nav--item', {
          active,
          [`${className}`]: className,
        })}
        data-testid={`${testID} ${title}`}
      >
        <a className="nav--item-icon" href={path}>
          <span className={`icon sidebar--icon ${icon}`} />
        </a>
        <div className="nav--item-menu">
          <a className="nav--item-header" href={path}>
            {title}
          </a>
          {children}
        </div>
      </div>
    )
  }
}

export default NavMenuItem
