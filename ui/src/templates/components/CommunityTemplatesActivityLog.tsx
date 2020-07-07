import React, {PureComponent} from 'react'

import {Table} from '@influxdata/clockface'

import {getStacks, Stack, TemplateKind} from 'src/client'

interface Props {
  orgID: string
}

interface State {
  stacks: Stack[]
}

interface Resource {
  apiVersion?: string
  resourceID?: string
  kind?: TemplateKind
  templateMetaName?: string
  associations?: {
    kind?: TemplateKind
    metaName?: string
  }[]
}

export class CommunityTemplatesActivityLog extends PureComponent<Props, State> {
  state = {
    stacks: [],
  }

  public async componentDidMount() {
    try {
      // todo: move the stacks reponse into redux
      const resp = await getStacks({query: {orgID: this.props.orgID}})

      const stacks = (resp.data as any).stacks
      this.setState({stacks})
    } catch (err) {
      console.error('error getting stacks', err)
    }
  }

  private renderStackResources(resources: Resource[]) {
    return resources.map(resource => {
      return (
        <React.Fragment key={resource.templateMetaName}>
          {resource.kind}
          <br />
        </React.Fragment>
      )
    })
  }

  private renderStackSources(sources: string[]) {
    return sources.map(source => {
      if (source.includes('github')) {
        return (
          <a key={source} href={source}>
            Github
          </a>
        )
      }

      return source
    })
  }

  render() {
    if (!this.state.stacks.length) {
      return <h4>You haven't installed any templates yet</h4>
    }

    return (
      <>
        <h2>Activity Log</h2>
        <Table striped={true} highlight={true}>
          <Table.Header>
            <Table.Row>
              <Table.HeaderCell>Template Name</Table.HeaderCell>
              <Table.HeaderCell>Resources Created</Table.HeaderCell>
              <Table.HeaderCell>Install Date</Table.HeaderCell>
              <Table.HeaderCell>Source</Table.HeaderCell>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {this.state.stacks.map(stack => {
              return (
                <Table.Row key={`stack-${stack.id}`}>
                  <Table.Cell>{stack.name}</Table.Cell>
                  <Table.Cell>
                    {this.renderStackResources(stack.resources)}
                  </Table.Cell>
                  <Table.Cell>
                    {new Date(stack.createdAt).toDateString()}
                  </Table.Cell>
                  <Table.Cell>
                    {this.renderStackSources(stack.sources)}
                  </Table.Cell>
                </Table.Row>
              )
            })}
          </Table.Body>
        </Table>
      </>
    )
  }
}
