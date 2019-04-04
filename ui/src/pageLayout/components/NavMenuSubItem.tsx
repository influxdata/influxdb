// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'
import classnames from 'classnames'

// Types
import {NavMenuType} from 'src/clockface'

interface Props {
  title: string
  path?: string
  active: boolean
  className?: string
  type: NavMenuType
  testID: string
  onClick?: () => void
}

class NavMenuSubItem extends PureComponent<Props> {
  public static defaultProps = {
    type: NavMenuType.RouterLink,
    testID: 'nav-menu--sub-item',
  }

  public render() {
    const {title, path, testID, type, active, className, onClick} = this.props

    if (type === NavMenuType.RouterLink) {
      return (
        <Link
          className={classnames('nav--sub-item', {
            active,
            [`${className}`]: className,
          })}
          data-testid={`${testID} ${title}`}
          to={path}
        >
          {title}
        </Link>
      )
    }

    if (type === NavMenuType.ShowDropdown) {
      return (
        <div className={`nav--sub-item ${className}`} onClick={onClick}>
          {title}
        </div>
      )
    }

    return (
      <a
        className={classnames('nav--sub-item', {
          active,
          [`${className}`]: className,
        })}
        data-testid={`${testID} ${title}`}
        href={path}
      >
        {title}
      </a>
    )
  }
}

export default NavMenuSubItem
