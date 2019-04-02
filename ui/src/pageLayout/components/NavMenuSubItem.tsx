// Libraries
import React, {Component} from 'react'
import {Link} from 'react-router'
import classnames from 'classnames'

// Types
import {NavMenuType} from 'src/clockface'

interface PassedProps {
  title: string
  path: string
  active: boolean
  className?: string
}

interface DefaultProps {
  type?: NavMenuType
  testID?: string
}

type Props = PassedProps & Partial<DefaultProps>

class NavMenuSubItem extends Component<Props> {
  public static defaultProps: DefaultProps = {
    type: NavMenuType.RouterLink,
    testID: 'nav-menu--sub-item',
  }

  public render() {
    const {title, path, testID, type, active, className} = this.props

    if (type === NavMenuType.RouterLink) {
      return (
        <Link
          className={classnames('nav--sub-item', {
            active,
            [`${className}`]: className,
          })}
          to={path}
          data-testid={`${testID} ${title}`}
        >
          {title}
        </Link>
      )
    }

    return (
      <a
        className={classnames('nav--sub-item', {active})}
        href={path}
        data-testid={testID}
      >
        {title}
      </a>
    )
  }
}

export default NavMenuSubItem
