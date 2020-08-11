// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {
  Table,
  ComponentSize,
  ButtonShape,
  ComponentColor,
  IconFont,
  Alignment,
} from '@influxdata/clockface'
import CopyButton from 'src/shared/components/CopyButton'

// Utils
import {getAll} from 'src/resources/selectors'

// Types
import {AppState, ResourceType, Authorization} from 'src/types'

type Props = ConnectedProps<typeof connector>

const WriteDataHelperTokens: FC<Props> = ({tokens}) => {
  return (
    <Table fontSize={ComponentSize.Small}>
      <Table.Header>
        <Table.Row>
          <Table.HeaderCell style={{width: '80%'}}>Tokens</Table.HeaderCell>
          <Table.HeaderCell style={{width: '20%'}} />
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {tokens.map(token => {
          return (
            <Table.Row key={token.id}>
              <Table.Cell>{token.description}</Table.Cell>
              <Table.Cell horizontalAlignment={Alignment.Right}>
                <CopyButton
                  shape={ButtonShape.Square}
                  icon={IconFont.Duplicate}
                  size={ComponentSize.ExtraSmall}
                  color={ComponentColor.Default}
                  contentName={token.description}
                  textToCopy={token.token}
                  testID={`copy-token ${token.description}`}
                />
              </Table.Cell>
            </Table.Row>
          )
        })}
      </Table.Body>
    </Table>
  )
}

const mstp = (state: AppState) => ({
  tokens: getAll<Authorization>(state, ResourceType.Authorizations),
})

const connector = connect(mstp)

export default connector(WriteDataHelperTokens)
