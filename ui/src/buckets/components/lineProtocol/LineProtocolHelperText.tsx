// Libraries
import React, {FC} from 'react'

const LineProtocolHelperText: FC<{}> = () => {
  return (
    <p>
      Need help writing InfluxDB Line Protocol?{' '}
      <a
        href="https://v2.docs.influxdata.com/v2.0/reference/syntax/line-protocol/"
        target="_blank"
      >
        See Documentation
      </a>
    </p>
  )
}

export default LineProtocolHelperText
