// Libraries
import React, {FC, useContext} from 'react'

// Contexts
import {WriteDataDetailsContext} from 'src/writeData/components/WriteDataDetailsContext'

// Components
import {
  List,
  ComponentSize,
  Heading,
  HeadingElement,
  Gradients,
  InfluxColors,
} from '@influxdata/clockface'

const WriteDataHelperTokens: FC = () => {
  const {token, tokens, changeToken} = useContext(WriteDataDetailsContext)

  return (
    <>
      <Heading
        element={HeadingElement.H6}
        className="write-data--details-widget-title"
      >
        Token
      </Heading>
      <List
        backgroundColor={InfluxColors.Obsidian}
        style={{height: '200px'}}
        maxHeight="200px"
      >
        {tokens.map(t => (
          <List.Item
            size={ComponentSize.Small}
            key={t.id}
            selected={t.id === token.id}
            value={t}
            onClick={changeToken}
            wrapText={true}
            gradient={Gradients.GundamPilot}
          >
            {t.description}
          </List.Item>
        ))}
      </List>
    </>
  )
}

export default WriteDataHelperTokens
