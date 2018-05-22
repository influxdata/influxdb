import React, {PureComponent, ReactElement} from 'react'
import {Link, withRouter, RouteComponentProps} from 'react-router'

import Dropdown from 'src/shared/components/Dropdown'
import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'
import {Source, Kapacitor} from 'src/types'
import {SetActiveKapacitor} from 'src/shared/actions/sources'

interface Props {
  source: Source
  kapacitors: Kapacitor[]
  setActiveKapacitor: SetActiveKapacitor
  deleteKapacitor: (Kapacitor: Kapacitor) => void
}

interface KapacitorItem {
  text: string
  resource: string
  kapacitor: Kapacitor
}

class KapacitorDropdown extends PureComponent<
  Props & RouteComponentProps<any, any>
> {
  public render() {
    const {source, router, setActiveKapacitor, deleteKapacitor} = this.props

    if (this.isKapacitorsEmpty) {
      return (
        <Authorized requiredRole={EDITOR_ROLE}>
          <Link
            to={`/sources/${source.id}/kapacitors/new`}
            className="btn btn-xs btn-default"
          >
            <span className="icon plus" /> Add Kapacitor Connection
          </Link>
        </Authorized>
      )
    }

    return (
      <Authorized
        requiredRole={EDITOR_ROLE}
        replaceWithIfNotAuthorized={this.UnauthorizedDropdown}
      >
        <Dropdown
          className="dropdown-260"
          buttonColor="btn-primary"
          buttonSize="btn-xs"
          items={this.kapacitorItems}
          onChoose={setActiveKapacitor}
          addNew={{
            url: `/sources/${source.id}/kapacitors/new`,
            text: 'Add Kapacitor Connection',
          }}
          actions={[
            {
              icon: 'pencil',
              text: 'edit',
              handler: item => {
                router.push(`${item.resource}/edit`)
              },
            },
            {
              icon: 'trash',
              text: 'delete',
              handler: item => {
                deleteKapacitor(item.kapacitor)
              },
              confirmable: true,
            },
          ]}
          selected={this.selected}
        />
      </Authorized>
    )
  }

  private get UnauthorizedDropdown(): ReactElement<HTMLDivElement> {
    return (
      <div className="source-table--kapacitor__view-only">{this.selected}</div>
    )
  }

  private get isKapacitorsEmpty(): boolean {
    const {kapacitors} = this.props
    return !kapacitors || kapacitors.length === 0
  }

  private get kapacitorItems(): KapacitorItem[] {
    const {kapacitors, source} = this.props

    return kapacitors.map(k => {
      return {
        text: k.name,
        resource: `/sources/${source.id}/kapacitors/${k.id}`,
        kapacitor: k,
      }
    })
  }

  private get activeKapacitor(): Kapacitor {
    return this.props.kapacitors.find(k => k.active)
  }

  private get selected(): string {
    let selected = ''
    if (this.activeKapacitor) {
      selected = this.activeKapacitor.name
    } else {
      selected = this.kapacitorItems[0].text
    }

    return selected
  }
}

export default withRouter<Props>(KapacitorDropdown)
