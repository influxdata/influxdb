import _ from 'lodash'

const emptyFunny = [
  'Looks like you don’t have any queries. Be a lot cooler if you did.',
  'Create a query. Go on, I dare ya!',
  'Create a query. Have fun!',
]

export const emptyGraphCopy = _.sample(emptyFunny)
