import React from 'react'
import {shallow} from 'enzyme'

import PageHeader from 'src/shared/components/PageHeader'

describe('PageHeader', () => {
  it('should throw an error if neither titleText nor titleComponents is supplied', () => {
    expect(() => shallow(<PageHeader />)).toThrow(
      'PageHeader requires either titleText or titleComponents prop'
    )
  })
})
