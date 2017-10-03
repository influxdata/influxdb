import _ from 'lodash'

const emptyFunny = [
  `Looks like you don't have any queries. Be a lot cooler if you did.`,
  'Create a query below. Go on, I dare ya!',
  'Create a query below. Have fun!',
  '1) Create a query below\n2) Profit',
]

export const emptyGraphCopy = _.sample(emptyFunny)
